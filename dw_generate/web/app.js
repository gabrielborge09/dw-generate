const logBox = document.getElementById("log-box");
const schedulerStateBox = document.getElementById("scheduler-state");
const forcedTaskBox = document.getElementById("forced-task");
const serverTime = document.getElementById("server-time");
const jobFormResult = document.getElementById("job-form-result");
const executionLogBox = document.getElementById("execution-log-box");

const BR_TIMEZONE = "America/Sao_Paulo";

function setJobFormResult(message, payload = null) {
  jobFormResult.textContent = payload ? `${message}\n${JSON.stringify(payload, null, 2)}` : message;
}

function datePartsInTimezone(date, timezone) {
  const parts = new Intl.DateTimeFormat("sv-SE", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const values = {};
  for (const item of parts) {
    values[item.type] = item.value;
  }
  return values;
}

function isoToDatetimeLocalBrt(isoValue) {
  if (!isoValue) {
    return "";
  }
  const date = new Date(isoValue);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const p = datePartsInTimezone(date, BR_TIMEZONE);
  return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}`;
}

function formatDateTimeBrt(isoValue) {
  if (!isoValue) {
    return "-";
  }
  const date = new Date(isoValue);
  if (Number.isNaN(date.getTime())) {
    return String(isoValue);
  }
  return new Intl.DateTimeFormat("pt-BR", {
    timeZone: BR_TIMEZONE,
    dateStyle: "short",
    timeStyle: "medium",
  }).format(date);
}

function defaultDailyDatetimeBrt() {
  const date = new Date(Date.now() + 30 * 60 * 1000);
  date.setSeconds(0, 0);
  return isoToDatetimeLocalBrt(date.toISOString());
}

function log(message, payload = null) {
  const line = `[${new Date().toISOString()}] ${message}`;
  const full = payload ? `${line}\n${JSON.stringify(payload, null, 2)}` : line;
  logBox.textContent = `${full}\n\n${logBox.textContent}`.slice(0, 14000);
}

async function apiFetch(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.detail || JSON.stringify(data));
  }
  return data;
}

function readForcedPayload() {
  const rowsValue = document.getElementById("forced-rows").value;
  return {
    rows: rowsValue ? Number(rowsValue) : null,
    continue_on_error: document.getElementById("forced-continue-on-error").checked,
    skip_validation: document.getElementById("forced-skip-validation").checked,
  };
}

function renderSchedulerState(status) {
  const brt = status.server_time_brt || formatDateTimeBrt(status.server_time_utc);
  serverTime.textContent = `BRT: ${brt} | UTC: ${status.server_time_utc}`;
  schedulerStateBox.textContent = JSON.stringify(status.scheduler, null, 2);
  forcedTaskBox.textContent = status.execution.current_task
    ? JSON.stringify(status.execution.current_task, null, 2)
    : "Nenhuma execucao manual em andamento.";
}

function renderJobs(jobs) {
  const tbody = document.getElementById("jobs-table-body");
  tbody.innerHTML = "";

  for (const job of jobs) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${job.id}</td>
      <td>${job.name}</td>
      <td>${job.mode}</td>
      <td>${job.schedule_type === "interval"
        ? `${job.interval_seconds}s`
        : `daily ${job.daily_time} (BRT)`}</td>
      <td>${formatDateTimeBrt(job.next_run_at)}</td>
      <td>${job.enabled ? "enabled" : "disabled"} / ${job.last_status || "-"}</td>
      <td></td>
    `;

    const actionsTd = tr.querySelector("td:last-child");
    const btnTrigger = document.createElement("button");
    btnTrigger.textContent = "Trigger";
    btnTrigger.type = "button";
    btnTrigger.onclick = () => triggerJob(job.id);
    actionsTd.appendChild(btnTrigger);

    const btnToggle = document.createElement("button");
    btnToggle.className = "ghost";
    btnToggle.textContent = job.enabled ? "Disable" : "Enable";
    btnToggle.type = "button";
    btnToggle.onclick = () => setJobEnabled(job.id, !job.enabled);
    actionsTd.appendChild(btnToggle);

    const btnEdit = document.createElement("button");
    btnEdit.className = "ghost";
    btnEdit.textContent = "Editar";
    btnEdit.type = "button";
    btnEdit.onclick = () => loadJobIntoForm(job);
    actionsTd.appendChild(btnEdit);

    tbody.appendChild(tr);
  }
}

function renderHistory(payload) {
  const schedulerBody = document.getElementById("scheduler-history-body");
  schedulerBody.innerHTML = "";
  for (const run of payload.scheduler_runs || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${run.id}</td>
      <td>${run.job_name}</td>
      <td>${formatDateTimeBrt(run.started_at)}</td>
      <td>${formatDateTimeBrt(run.finished_at)}</td>
      <td>${run.status}</td>
      <td>${run.error_text || "-"}</td>
    `;
    schedulerBody.appendChild(tr);
  }

  const manualBody = document.getElementById("manual-history-body");
  manualBody.innerHTML = "";
  for (const run of payload.manual_runs || []) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${run.id}</td>
      <td>${run.mode}</td>
      <td>${formatDateTimeBrt(run.started_at)}</td>
      <td>${formatDateTimeBrt(run.finished_at)}</td>
      <td>${run.status}</td>
      <td>${run.error_text || "-"}</td>
    `;
    manualBody.appendChild(tr);
  }
}

function renderExecutionLogs(entries) {
  if (!entries || entries.length === 0) {
    executionLogBox.textContent = "Sem logs de execucao ainda.";
    return;
  }
  executionLogBox.textContent = entries.map((entry) => {
    const when = entry.timestamp_brt || entry.timestamp_utc || "-";
    const event = entry.event_type || "event";
    const payload = entry.payload ? JSON.stringify(entry.payload, null, 2) : "{}";
    return `[${when}] ${event}\n${payload}`;
  }).join("\n\n");
}

async function refreshStatus() {
  const status = await apiFetch("/api/status");
  renderSchedulerState(status);
}

async function refreshJobs() {
  const data = await apiFetch("/api/jobs?include_disabled=true");
  renderJobs(data.jobs || []);
}

async function refreshHistory() {
  const data = await apiFetch("/api/history?limit=25");
  renderHistory(data);
}

async function refreshExecutionLogs() {
  const data = await apiFetch("/api/logs?limit=100");
  renderExecutionLogs(data.logs || []);
}

async function startScheduler() {
  const poll = document.getElementById("poll-seconds").value;
  const maxJobs = document.getElementById("max-jobs-per-cycle").value;
  const payload = {
    poll_seconds: poll ? Number(poll) : null,
    max_jobs_per_cycle: maxJobs ? Number(maxJobs) : null,
  };
  const result = await apiFetch("/api/scheduler/start", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  log("Scheduler iniciado", result.scheduler);
  await refreshStatus();
}

async function stopScheduler() {
  const result = await apiFetch("/api/scheduler/stop", { method: "POST" });
  log("Scheduler parado", result.scheduler);
  await refreshStatus();
}

async function runOnce() {
  const result = await apiFetch("/api/scheduler/run-once", { method: "POST" });
  log("Run once executado", result);
  await Promise.all([refreshStatus(), refreshHistory(), refreshExecutionLogs()]);
}

async function forceFull() {
  const payload = readForcedPayload();
  const result = await apiFetch("/api/execute/full", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  log("Full forcado iniciado", result);
  await Promise.all([refreshStatus(), refreshExecutionLogs()]);
}

async function forceIncremental() {
  const payload = readForcedPayload();
  const result = await apiFetch("/api/execute/incremental", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  log("Incremental forcado iniciado", result);
  await Promise.all([refreshStatus(), refreshExecutionLogs()]);
}

async function triggerJob(jobRef) {
  const result = await apiFetch(`/api/jobs/${jobRef}/trigger`, { method: "POST" });
  const runInfo = result && result.triggered ? result.triggered : result;
  log(`Job ${jobRef} disparado`, runInfo);
  await Promise.all([refreshJobs(), refreshHistory(), refreshStatus(), refreshExecutionLogs()]);
}

async function setJobEnabled(jobRef, enabled) {
  const path = enabled ? "enable" : "disable";
  const result = await apiFetch(`/api/jobs/${jobRef}/${path}`, { method: "POST" });
  log(`Job ${jobRef} ${enabled ? "habilitado" : "desabilitado"}`, result);
  await refreshJobs();
}

function syncJobScheduleFields() {
  const scheduleType = document.getElementById("job-schedule-type").value;
  const intervalBlock = document.getElementById("job-interval-fields");
  const dailyBlock = document.getElementById("job-daily-fields");
  const intervalValue = document.getElementById("job-interval-value");
  const intervalUnit = document.getElementById("job-interval-unit");
  const dailyAt = document.getElementById("job-daily-at");

  if (scheduleType === "interval") {
    intervalBlock.classList.remove("hidden");
    dailyBlock.classList.add("hidden");
    intervalValue.disabled = false;
    intervalUnit.disabled = false;
    dailyAt.disabled = true;
    return;
  }

  intervalBlock.classList.add("hidden");
  dailyBlock.classList.remove("hidden");
  intervalValue.disabled = true;
  intervalUnit.disabled = true;
  dailyAt.disabled = false;
  if (!dailyAt.value) {
    dailyAt.value = defaultDailyDatetimeBrt();
  }
}

function clearJobForm() {
  document.getElementById("job-name").value = "";
  document.getElementById("job-mode").value = "incremental";
  document.getElementById("job-schedule-type").value = "interval";
  document.getElementById("job-interval-value").value = "30";
  document.getElementById("job-interval-unit").value = "minutes";
  document.getElementById("job-daily-at").value = "";
  document.getElementById("job-rows").value = "";
  document.getElementById("job-continue-on-error").checked = false;
  document.getElementById("job-skip-validation").checked = false;
  document.getElementById("job-enabled").checked = true;
  document.getElementById("job-replace").checked = true;
  setJobFormResult("Formulario limpo.");
  syncJobScheduleFields();
}

function loadJobIntoForm(job) {
  document.getElementById("job-name").value = job.name || "";
  document.getElementById("job-mode").value = job.mode || "incremental";
  document.getElementById("job-rows").value = job.rows_override || "";
  document.getElementById("job-continue-on-error").checked = Boolean(job.continue_on_error);
  document.getElementById("job-skip-validation").checked = Boolean(job.skip_validation);
  document.getElementById("job-enabled").checked = Boolean(job.enabled);
  document.getElementById("job-replace").checked = true;

  if (job.schedule_type === "daily") {
    document.getElementById("job-schedule-type").value = "daily";
    document.getElementById("job-daily-at").value = isoToDatetimeLocalBrt(job.next_run_at);
  } else {
    document.getElementById("job-schedule-type").value = "interval";
    const intervalSeconds = Number(job.interval_seconds || 1800);
    if (intervalSeconds % 3600 === 0) {
      document.getElementById("job-interval-unit").value = "hours";
      document.getElementById("job-interval-value").value = String(intervalSeconds / 3600);
    } else {
      document.getElementById("job-interval-unit").value = "minutes";
      document.getElementById("job-interval-value").value = String(Math.max(1, Math.floor(intervalSeconds / 60)));
    }
  }

  syncJobScheduleFields();
  setJobFormResult(`Job ${job.name} carregada para edicao.`);
  const card = document.getElementById("job-form-card");
  if (card) {
    card.scrollIntoView({ behavior: "smooth", block: "start" });
  }
  document.getElementById("job-name").focus();
}

function readJobPayload() {
  const scheduleType = document.getElementById("job-schedule-type").value;
  const rowsValue = document.getElementById("job-rows").value;
  const payload = {
    name: document.getElementById("job-name").value.trim(),
    mode: document.getElementById("job-mode").value,
    schedule_type: scheduleType,
    interval_unit: null,
    interval_value: null,
    daily_at: null,
    rows: rowsValue ? Number(rowsValue) : null,
    continue_on_error: document.getElementById("job-continue-on-error").checked,
    skip_validation: document.getElementById("job-skip-validation").checked,
    enabled: document.getElementById("job-enabled").checked,
    replace: document.getElementById("job-replace").checked,
  };

  if (!payload.name) {
    throw new Error("Nome da job e obrigatorio.");
  }

  if (scheduleType === "interval") {
    const intervalValueText = document.getElementById("job-interval-value").value;
    const intervalValue = intervalValueText ? Number(intervalValueText) : 0;
    if (!intervalValue || intervalValue < 1) {
      throw new Error("Intervalo deve ser >= 1.");
    }
    payload.interval_unit = document.getElementById("job-interval-unit").value;
    payload.interval_value = intervalValue;
  } else {
    const dailyAt = document.getElementById("job-daily-at").value;
    if (!dailyAt) {
      throw new Error("Data/hora inicial (BRT) e obrigatoria para agenda daily.");
    }
    payload.daily_at = dailyAt;
  }

  return payload;
}

async function saveJob() {
  try {
    const payload = readJobPayload();
    const result = await apiFetch("/api/jobs", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    setJobFormResult("Job salva com sucesso.", result);
    log("Job salva", result);
    await refreshJobs();
  } catch (error) {
    setJobFormResult(`Erro ao salvar job: ${error.message}`);
    throw error;
  }
}

function bindActions() {
  document.getElementById("btn-start-scheduler").onclick = () => safeRun(startScheduler);
  document.getElementById("btn-stop-scheduler").onclick = () => safeRun(stopScheduler);
  document.getElementById("btn-run-once").onclick = () => safeRun(runOnce);
  document.getElementById("btn-force-full").onclick = () => safeRun(forceFull);
  document.getElementById("btn-force-incremental").onclick = () => safeRun(forceIncremental);
  document.getElementById("btn-refresh-jobs").onclick = () => safeRun(refreshJobs);
  document.getElementById("btn-refresh-history").onclick = () => safeRun(refreshHistory);
  document.getElementById("btn-refresh-execution-logs").onclick = () => safeRun(refreshExecutionLogs);
  document.getElementById("btn-save-job").onclick = () => safeRun(saveJob);
  document.getElementById("btn-clear-job-form").onclick = () => safeRun(async () => clearJobForm());
  document.getElementById("job-schedule-type").onchange = syncJobScheduleFields;
}

async function safeRun(fn) {
  try {
    await fn();
  } catch (error) {
    log("Erro", { error: error.message });
  }
}

async function boot() {
  bindActions();
  clearJobForm();
  await safeRun(async () => {
    await Promise.all([refreshStatus(), refreshJobs(), refreshHistory(), refreshExecutionLogs()]);
  });
  setInterval(() => safeRun(refreshStatus), 5000);
  setInterval(() => safeRun(refreshHistory), 10000);
  setInterval(() => safeRun(refreshExecutionLogs), 6000);
}

boot();
