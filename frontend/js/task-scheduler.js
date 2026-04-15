const API_BASE = window.location.origin;

let tasks = [];
let refreshInterval = null;

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
    loadTasks();
    refreshInterval = setInterval(loadTasks, 15000);
});

// ---------------------------------------------------------------------------
// Load & render tasks
// ---------------------------------------------------------------------------
async function loadTasks() {
    try {
        const res = await fetch(`${API_BASE}/api/task-scheduler/tasks`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        tasks = data.tasks || [];
        renderStats();
        renderTable();
    } catch (e) {
        console.error('Failed to load tasks:', e);
    }
}

function renderStats() {
    const total = tasks.length;
    const active = tasks.filter(t => t.is_enabled).length;
    const disabled = total - active;

    document.getElementById('statTotal').textContent = total;
    document.getElementById('statActive').textContent = active;
    document.getElementById('statDisabled').textContent = disabled;

    // Last run info
    const lastRuns = tasks
        .filter(t => t.last_run_at)
        .sort((a, b) => new Date(b.last_run_at) - new Date(a.last_run_at));

    if (lastRuns.length > 0) {
        const lr = lastRuns[0];
        const statusIcon = lr.last_run_status === 'completed' ? '✓' : '✗';
        document.getElementById('statLastRun').textContent = statusIcon;
        document.getElementById('statLastRun').className =
            `stat-value ${lr.last_run_status === 'completed' ? 'text-success' : 'text-danger'}`;
    }
}

function renderTable() {
    const tbody = document.getElementById('taskTableBody');
    if (tasks.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted py-4">Geen taken gevonden. Klik op "+ Nieuwe taak" of importeer bestaande taken.</td></tr>';
        return;
    }

    tbody.innerHTML = tasks.map(t => {
        const statusBadge = t.is_enabled
            ? `<span class="badge badge-enabled">Actief</span>`
            : `<span class="badge badge-disabled">Uitgeschakeld</span>`;

        const winStatus = t.win_status || 'Unknown';

        const schedule = formatSchedule(t.schedule_type, t.schedule_time, t.schedule_days);

        const lastRun = t.last_run_at
            ? `<span class="badge badge-${t.last_run_status || 'completed'}">${t.last_run_status || '-'}</span> ${formatDate(t.last_run_at)}`
            : '<span class="text-muted">-</span>';

        const nextRun = t.next_run_time && t.next_run_time !== 'N/A'
            ? t.next_run_time
            : '<span class="text-muted">-</span>';

        return `
            <tr>
                <td>
                    <strong>${escapeHtml(t.display_name)}</strong>
                    <br><small class="text-muted">${escapeHtml(t.task_name)}</small>
                </td>
                <td class="schedule-text">${schedule}</td>
                <td>${statusBadge}</td>
                <td><small>${nextRun}</small></td>
                <td><small>${lastRun}</small></td>
                <td class="text-center">
                    <button class="btn btn-outline-primary btn-action" onclick="runTask(${t.id})" title="Nu uitvoeren">
                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24"><polygon points="5 3 19 12 5 21 5 3"/></svg>
                    </button>
                    <button class="btn btn-outline-secondary btn-action" onclick="showHistory(${t.id}, '${escapeHtml(t.display_name)}')" title="Geschiedenis">
                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                    </button>
                    <button class="btn btn-outline-warning btn-action" onclick="toggleTask(${t.id})" title="${t.is_enabled ? 'Uitschakelen' : 'Inschakelen'}">
                        ${t.is_enabled
                            ? '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>'
                            : '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24"><polygon points="5 3 19 12 5 21 5 3"/></svg>'}
                    </button>
                    <button class="btn btn-outline-info btn-action" onclick="openEditModal(${t.id})" title="Bewerken">
                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
                    </button>
                    <button class="btn btn-outline-danger btn-action" onclick="confirmDelete(${t.id}, '${escapeHtml(t.display_name)}')" title="Verwijderen">
                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                    </button>
                </td>
            </tr>
        `;
    }).join('');
}

function formatSchedule(type, time, days) {
    if (type === 'DAILY') return `Dagelijks om ${time}`;
    if (type === 'WEEKLY') return `Wekelijks ${days || ''} om ${time}`;
    if (type === 'HOURLY') return `Elk uur om :${time.split(':')[1] || '00'}`;
    return `${type} ${time}`;
}

function formatDate(iso) {
    if (!iso) return '-';
    const d = new Date(iso);
    return d.toLocaleDateString('nl-NL', { day: '2-digit', month: '2-digit', year: 'numeric' })
        + ' ' + d.toLocaleTimeString('nl-NL', { hour: '2-digit', minute: '2-digit' });
}

function formatDuration(start, end) {
    if (!start || !end) return '-';
    const ms = new Date(end) - new Date(start);
    const secs = Math.floor(ms / 1000);
    if (secs < 60) return `${secs}s`;
    const mins = Math.floor(secs / 60);
    if (mins < 60) return `${mins}m ${secs % 60}s`;
    const hrs = Math.floor(mins / 60);
    return `${hrs}u ${mins % 60}m`;
}

function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ---------------------------------------------------------------------------
// Create / Edit
// ---------------------------------------------------------------------------
function openCreateModal() {
    document.getElementById('modalTitle').textContent = 'Nieuwe taak';
    document.getElementById('editTaskId').value = '';
    document.getElementById('inputTaskName').value = '';
    document.getElementById('inputTaskName').disabled = false;
    document.getElementById('inputDisplayName').value = '';
    document.getElementById('inputDescription').value = '';
    document.getElementById('inputCommand').value = '';
    document.getElementById('inputWorkDir').value = 'C:\\Users\\l.davidowski\\dm-dashboard';
    document.getElementById('inputScheduleType').value = 'DAILY';
    document.getElementById('inputScheduleTime').value = '07:00';
    document.getElementById('inputScheduleDays').value = '';
    toggleDaysInput();
}

function openEditModal(taskId) {
    const task = tasks.find(t => t.id === taskId);
    if (!task) return;

    document.getElementById('modalTitle').textContent = 'Taak bewerken';
    document.getElementById('editTaskId').value = taskId;
    document.getElementById('inputTaskName').value = task.task_name;
    document.getElementById('inputTaskName').disabled = true; // Can't change slug
    document.getElementById('inputDisplayName').value = task.display_name;
    document.getElementById('inputDescription').value = task.description || '';
    document.getElementById('inputCommand').value = task.command;
    document.getElementById('inputWorkDir').value = task.working_directory || '';
    document.getElementById('inputScheduleType').value = task.schedule_type;
    document.getElementById('inputScheduleTime').value = task.schedule_time;
    document.getElementById('inputScheduleDays').value = task.schedule_days || '';
    toggleDaysInput();

    new bootstrap.Modal(document.getElementById('taskModal')).show();
}

function toggleDaysInput() {
    const type = document.getElementById('inputScheduleType').value;
    document.getElementById('daysGroup').classList.toggle('d-none', type !== 'WEEKLY');
}

async function saveTask() {
    const taskId = document.getElementById('editTaskId').value;
    const data = {
        task_name: document.getElementById('inputTaskName').value.trim(),
        display_name: document.getElementById('inputDisplayName').value.trim(),
        description: document.getElementById('inputDescription').value.trim(),
        command: document.getElementById('inputCommand').value.trim(),
        working_directory: document.getElementById('inputWorkDir').value.trim(),
        schedule_type: document.getElementById('inputScheduleType').value,
        schedule_time: document.getElementById('inputScheduleTime').value,
        schedule_days: document.getElementById('inputScheduleDays').value.trim() || null,
    };

    if (!data.task_name || !data.display_name || !data.command) {
        alert('Vul minimaal taaknaam, weergavenaam en commando in.');
        return;
    }

    try {
        const url = taskId
            ? `${API_BASE}/api/task-scheduler/tasks/${taskId}`
            : `${API_BASE}/api/task-scheduler/tasks`;
        const method = taskId ? 'PUT' : 'POST';

        const res = await fetch(url, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });

        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || `HTTP ${res.status}`);
        }

        bootstrap.Modal.getInstance(document.getElementById('taskModal'))?.hide();
        await loadTasks();
    } catch (e) {
        alert(`Fout bij opslaan: ${e.message}`);
    }
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------
async function toggleTask(taskId) {
    try {
        const res = await fetch(`${API_BASE}/api/task-scheduler/tasks/${taskId}/toggle`, { method: 'POST' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        await loadTasks();
    } catch (e) {
        alert(`Fout: ${e.message}`);
    }
}

async function runTask(taskId) {
    const task = tasks.find(t => t.id === taskId);
    if (!confirm(`Wil je "${task?.display_name || 'deze taak'}" nu handmatig uitvoeren?`)) return;

    try {
        const res = await fetch(`${API_BASE}/api/task-scheduler/tasks/${taskId}/run`, { method: 'POST' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        alert(`Taak gestart (run #${data.run_id}). Bekijk de geschiedenis voor voortgang.`);
        await loadTasks();
    } catch (e) {
        alert(`Fout: ${e.message}`);
    }
}

async function confirmDelete(taskId, name) {
    if (!confirm(`Weet je zeker dat je "${name}" wilt verwijderen? Dit verwijdert ook de Windows-taak.`)) return;

    try {
        const res = await fetch(`${API_BASE}/api/task-scheduler/tasks/${taskId}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        document.getElementById('historySection').classList.add('d-none');
        await loadTasks();
    } catch (e) {
        alert(`Fout: ${e.message}`);
    }
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------
async function showHistory(taskId, taskName) {
    document.getElementById('historySection').classList.remove('d-none');
    document.getElementById('historyTitle').textContent = `Uitvoeringsgeschiedenis — ${taskName}`;

    try {
        const res = await fetch(`${API_BASE}/api/task-scheduler/tasks/${taskId}/runs`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const runs = data.runs || [];

        const tbody = document.getElementById('historyTableBody');
        if (runs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-3">Nog geen uitvoeringen.</td></tr>';
            return;
        }

        tbody.innerHTML = runs.map(r => `
            <tr>
                <td><small>${formatDate(r.started_at)}</small></td>
                <td><small>${formatDuration(r.started_at, r.completed_at)}</small></td>
                <td><span class="badge badge-${r.status}">${r.status}</span></td>
                <td><small>${r.trigger_type}</small></td>
                <td class="text-end">
                    <button class="btn btn-outline-secondary btn-action" onclick="viewLog(${r.id})">Bekijk log</button>
                </td>
            </tr>
        `).join('');
    } catch (e) {
        console.error('Failed to load history:', e);
    }
}

async function viewLog(runId) {
    const logEl = document.getElementById('logOutput');
    logEl.textContent = 'Laden...';
    new bootstrap.Modal(document.getElementById('logModal')).show();

    try {
        const res = await fetch(`${API_BASE}/api/task-scheduler/runs/${runId}/log`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        logEl.textContent = data.output_log || '(geen output)';
        if (data.error_message) {
            logEl.textContent += '\n\n--- ERROR ---\n' + data.error_message;
        }
    } catch (e) {
        logEl.textContent = `Fout bij laden: ${e.message}`;
    }
}

// ---------------------------------------------------------------------------
// Import existing Windows tasks
// ---------------------------------------------------------------------------
async function importExisting() {
    try {
        const res = await fetch(`${API_BASE}/api/task-scheduler/import-existing`, { method: 'POST' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        alert(`${data.imported} taak/taken geimporteerd.`);
        document.getElementById('importBanner').classList.add('d-none');
        await loadTasks();
    } catch (e) {
        alert(`Fout bij importeren: ${e.message}`);
    }
}
