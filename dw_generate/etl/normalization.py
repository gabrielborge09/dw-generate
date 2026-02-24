from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple

from ..core.config import NormalizationConfig


_SELECT_BLOCK_RE = re.compile(r"\bSELECT\b(?P<body>.*?)\bFROM\b", re.IGNORECASE | re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--.*?$", re.MULTILINE)
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_ALIAS_WITH_AS_RE = re.compile(
    r"(?is)^(?P<expr>.+?)\s+AS\s+(?P<alias>\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)\s*$"
)
_BARE_IDENTIFIER_RE = re.compile(
    r"^\s*(?:\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)){0,2}\s*$"
)


@dataclass
class SqlValidationResult:
    valid: bool
    errors: List[str]


def validate_sql_server_script(
    sql_text: str, cfg: NormalizationConfig, layer_name: str | None = None
) -> SqlValidationResult:
    if not cfg.enabled:
        return SqlValidationResult(valid=True, errors=[])

    normalized_layer = (layer_name or "").strip().lower()
    strict_normalization = normalized_layer in {"", "stage"}
    min_required_normalized = cfg.min_normalized_columns_per_select if strict_normalization else 0

    errors: List[str] = []
    clean_sql = _strip_comments(sql_text)
    lowered = clean_sql.lower()

    if cfg.require_create_table and not re.search(r"\bcreate\s+table\b", lowered):
        errors.append("Script sem CREATE TABLE.")

    if cfg.require_insert_select and not _has_insert_select_or_merge(clean_sql):
        errors.append("Script deve conter INSERT ... SELECT (ou MERGE ... USING/SELECT).")

    if cfg.forbid_destructive_commands:
        destructive_patterns = [
            r"\bdrop\s+table\b",
            r"\btruncate\s+table\b",
            r"\bdelete\s+from\b",
        ]
        for pattern in destructive_patterns:
            if re.search(pattern, lowered):
                errors.append(f"Comando destrutivo proibido detectado: `{pattern}`.")

    if re.search(r"\bcreate\s+table\s+if\s+not\s+exists\b", lowered):
        errors.append("SQL Server nao suporta CREATE TABLE IF NOT EXISTS.")
    if re.search(r"\blimit\s+\d+\b", lowered):
        errors.append("LIMIT detectado; em SQL Server use TOP.")
    if re.search(r"::\s*[a-zA-Z_]", clean_sql):
        errors.append("Cast estilo PostgreSQL (`::tipo`) detectado.")

    select_blocks = list(_SELECT_BLOCK_RE.finditer(clean_sql))
    if not select_blocks:
        errors.append("Nenhum bloco SELECT ... FROM detectado.")
    else:
        for block_idx, match in enumerate(select_blocks, start=1):
            body = _strip_select_modifiers(match.group("body"))
            projections = _split_top_level_csv(body)
            if not projections:
                errors.append(f"SELECT #{block_idx} sem colunas projetadas.")
                continue

            normalized_count = 0
            for col_idx, projection in enumerate(projections, start=1):
                if cfg.forbid_select_star and _projection_has_star(projection):
                    errors.append(f"SELECT #{block_idx}, coluna #{col_idx}: SELECT * proibido.")
                    continue

                expr, alias, alias_error = _extract_alias(projection, cfg.require_explicit_alias)
                if alias_error:
                    errors.append(f"SELECT #{block_idx}, coluna #{col_idx}: {alias_error}")
                    continue

                alias_unquoted = alias.strip()[1:-1] if alias.strip().startswith("[") else alias.strip()
                if not re.fullmatch(cfg.alias_regex, alias_unquoted):
                    errors.append(
                        f"SELECT #{block_idx}, coluna #{col_idx}: alias fora do padrao `{cfg.alias_regex}`: `{alias_unquoted}`."
                    )

                if _is_normalized_expression(expr, cfg.required_functions_any):
                    normalized_count += 1
                else:
                    if not strict_normalization and _BARE_IDENTIFIER_RE.fullmatch(expr):
                        continue
                    passthrough = {item.lower() for item in cfg.passthrough_aliases}
                    if alias_unquoted.lower() not in passthrough:
                        errors.append(
                            f"SELECT #{block_idx}, coluna #{col_idx}: expressao sem normalizacao detectada para alias `{alias_unquoted}`."
                        )

            if normalized_count < min_required_normalized:
                errors.append(
                    f"SELECT #{block_idx}: quantidade de colunas normalizadas ({normalized_count}) menor que o minimo ({min_required_normalized})."
                )

    return SqlValidationResult(valid=not errors, errors=errors)


def split_sql_server_batches(sql_text: str) -> List[str]:
    parts = re.split(r"(?im)^\s*GO\s*;?\s*$", sql_text)
    return [part.strip() for part in parts if part and part.strip()]


def _strip_comments(sql_text: str) -> str:
    no_block = _BLOCK_COMMENT_RE.sub("", sql_text)
    return _LINE_COMMENT_RE.sub("", no_block)


def _has_insert_select_or_merge(sql_text: str) -> bool:
    insert_select = re.search(
        r"\binsert\s+into\b[\s\S]*?\bselect\b", sql_text, re.IGNORECASE
    )
    merge_using = re.search(r"\bmerge\s+into\b[\s\S]*?\busing\b", sql_text, re.IGNORECASE)
    return bool(insert_select or merge_using)


def _projection_has_star(expr: str) -> bool:
    stripped = expr.strip()
    if stripped == "*":
        return True
    if re.fullmatch(r"(?:\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)\.\*", stripped):
        return True
    return False


def _is_normalized_expression(expr: str, required_functions_any: List[str]) -> bool:
    if not expr.strip():
        return False
    if not _BARE_IDENTIFIER_RE.fullmatch(expr):
        for fn in required_functions_any:
            if re.search(rf"\b{re.escape(fn)}\s*\(", expr, re.IGNORECASE):
                return True
        if re.search(r"\bcase\s+when\b", expr, re.IGNORECASE):
            return True
        if re.search(r"\+", expr) and not re.search(r"^\s*\d+\s*\+\s*\d+\s*$", expr):
            return True
        return False
    return False


def _extract_alias(projection: str, require_explicit_alias: bool) -> Tuple[str, str, str | None]:
    text = projection.strip()
    if not text:
        return "", "", "projecao vazia."

    if require_explicit_alias:
        match = _ALIAS_WITH_AS_RE.match(text)
        if not match:
            return "", "", "alias explicito com AS e obrigatorio."
        return match.group("expr").strip(), match.group("alias").strip(), None

    match = _ALIAS_WITH_AS_RE.match(text)
    if match:
        return match.group("expr").strip(), match.group("alias").strip(), None

    no_alias_match = re.match(r"(?is)^(?P<expr>.+?)\s+(?P<alias>\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)\s*$", text)
    if no_alias_match:
        return no_alias_match.group("expr").strip(), no_alias_match.group("alias").strip(), None

    return text, "", "alias nao identificado."


def _strip_select_modifiers(body: str) -> str:
    text = body.strip()
    changed = True
    while changed:
        before = text
        text = re.sub(r"^\s*DISTINCT\s+", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^\s*ALL\s+", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^\s*TOP\s*\(\s*\d+\s*\)\s*(?:PERCENT\s*)?(?:WITH\s+TIES\s*)?", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^\s*TOP\s+\d+\s*(?:PERCENT\s*)?(?:WITH\s+TIES\s*)?", "", text, flags=re.IGNORECASE)
        changed = before != text
    return text


def _split_top_level_csv(text: str) -> List[str]:
    items: List[str] = []
    current: List[str] = []
    depth = 0
    in_single = False
    in_double = False
    in_bracket = False
    i = 0

    while i < len(text):
        ch = text[i]

        if ch == "'" and not in_double and not in_bracket:
            if in_single and i + 1 < len(text) and text[i + 1] == "'":
                current.append(ch)
                current.append(text[i + 1])
                i += 2
                continue
            in_single = not in_single
            current.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single and not in_bracket:
            in_double = not in_double
            current.append(ch)
            i += 1
            continue

        if ch == "[" and not in_single and not in_double:
            in_bracket = True
            current.append(ch)
            i += 1
            continue

        if ch == "]" and in_bracket and not in_single and not in_double:
            in_bracket = False
            current.append(ch)
            i += 1
            continue

        if not in_single and not in_double and not in_bracket:
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
            elif ch == "," and depth == 0:
                piece = "".join(current).strip()
                if piece:
                    items.append(piece)
                current = []
                i += 1
                continue

        current.append(ch)
        i += 1

    last = "".join(current).strip()
    if last:
        items.append(last)
    return items
