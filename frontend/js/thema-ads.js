// Global state
let currentJobId = null;
let pollInterval = null;
let themes = [];

// Format date to local timezone
function formatDateTime(isoString) {
    if (!isoString) return 'Not started';

    const date = new Date(isoString);

    // Format: DD-MM-YYYY HH:MM:SS (Local Time)
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const year = date.getFullYear();
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');

    // Get timezone abbreviation
    const timeZone = date.toLocaleTimeString('en-US', { timeZoneName: 'short' }).split(' ')[2];

    return `${day}-${month}-${year} ${hours}:${minutes}:${seconds} ${timeZone}`;
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    loadThemes();
    loadQueueStatus();
    refreshJobs();
    loadActivationPlan();
    // Auto-refresh jobs every 5 seconds
    setInterval(refreshJobs, 5000);
    // Auto-refresh queue status every 10 seconds
    setInterval(loadQueueStatus, 10000);
});

async function loadThemes() {
    try {
        const response = await fetch('/api/thema-ads/themes');
        const data = await response.json();

        if (response.ok && data.themes) {
            themes = data.themes;

            // Update supported themes display with accepted input formats
            const themeAliases = {
                'black_friday': ['black_friday', 'bf', 'black friday'],
                'cyber_monday': ['cyber_monday', 'cm'],
                'sinterklaas': ['sinterklaas', 'sint'],
                'kerstmis': ['kerstmis', 'kerst', 'christmas', 'xmas'],
                'singles_day': ['singles_day', 'sd', 'singles']
            };
            const themesText = themes.map(t => {
                const aliases = themeAliases[t.name] || [t.name];
                return `${t.display_name} (${aliases.join(', ')})`;
            }).join(' | ');
            const supportedThemesEl = document.getElementById('supportedThemes');
            if (supportedThemesEl) {
                supportedThemesEl.textContent = themesText;
            }

            // Populate theme dropdown for Auto-Discover
            const themeSelect = document.getElementById('discoverTheme');
            if (themeSelect) {
                themeSelect.innerHTML = themes.map(t =>
                    `<option value="${t.name}" ${t.name === 'singles_day' ? 'selected' : ''}>${t.display_name}</option>`
                ).join('');
            }

            // Populate theme dropdown for CSV Upload
            const csvThemeSelect = document.getElementById('csvTheme');
            if (csvThemeSelect) {
                csvThemeSelect.innerHTML = themes.map(t =>
                    `<option value="${t.name}" ${t.name === 'singles_day' ? 'selected' : ''}>${t.display_name}</option>`
                ).join('');
            }

            // Populate theme checkboxes for Run All Themes
            const allThemesCheckboxes = document.getElementById('allThemesCheckboxes');
            if (allThemesCheckboxes) {
                allThemesCheckboxes.innerHTML = themes.map(t =>
                    `<div class="form-check">
                        <input class="form-check-input all-themes-checkbox" type="checkbox" value="${t.name}" id="theme_${t.name}" checked>
                        <label class="form-check-label" for="theme_${t.name}">
                            ${t.display_name}
                        </label>
                    </div>`
                ).join('');
            }
        }
    } catch (error) {
        console.error('Error loading themes:', error);
    }
}

async function uploadExcel() {
    const fileInput = document.getElementById('excelFile');
    const batchSize = document.getElementById('excelBatchSize').value;
    const resultDiv = document.getElementById('excelUploadResult');

    if (!fileInput.files.length) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please select an Excel file</div>';
        return;
    }

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    formData.append('batch_size', batchSize);

    resultDiv.innerHTML = '<div class="alert alert-info">Uploading...</div>';

    try {
        const response = await fetch('/api/thema-ads/upload-excel', {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (response.ok) {
            resultDiv.innerHTML = `
                <div class="alert alert-success">
                    <strong>Success!</strong> Job ${data.job_id} created with ${data.total_items} items.
                    Processing started automatically.
                </div>
            `;
            currentJobId = data.job_id;
            startPolling(data.job_id);
            fileInput.value = '';
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

async function uploadCSV() {
    const fileInput = document.getElementById('csvFile');
    const batchSize = document.getElementById('csvBatchSize').value;
    const theme = document.getElementById('csvTheme').value;
    const resultDiv = document.getElementById('uploadResult');

    if (!fileInput.files.length) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please select a CSV file</div>';
        return;
    }

    if (!theme) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please select a theme</div>';
        return;
    }

    const formData = new FormData();
    formData.append('file', fileInput.files[0]);
    formData.append('batch_size', batchSize);
    formData.append('theme', theme);

    resultDiv.innerHTML = '<div class="alert alert-info">Uploading...</div>';

    try {
        const response = await fetch('/api/thema-ads/upload', {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (response.ok) {
            resultDiv.innerHTML = `
                <div class="alert alert-success">
                    <strong>Success!</strong> Job ${data.job_id} created with ${data.total_items} items.
                    Processing started automatically.
                </div>
            `;
            currentJobId = data.job_id;
            startPolling(data.job_id);
            fileInput.value = '';
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

async function discoverAdGroups() {
    const limit = document.getElementById('discoverLimit').value;
    const batchSize = document.getElementById('discoverBatchSize').value;
    const jobChunkSize = document.getElementById('discoverJobChunkSize').value;
    const theme = document.getElementById('discoverTheme').value;
    const resultDiv = document.getElementById('discoverResult');
    const btn = document.getElementById('discoverBtn');

    btn.disabled = true;
    resultDiv.innerHTML = '<div class="alert alert-info">Discovering ad groups...</div>';

    try {
        const formData = new FormData();
        if (limit) formData.append('limit', limit);
        formData.append('batch_size', batchSize);
        formData.append('job_chunk_size', jobChunkSize);
        formData.append('theme', theme);

        const response = await fetch('/api/thema-ads/discover', {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (response.ok) {
            if (data.status === 'no_ad_groups_found') {
                resultDiv.innerHTML = `
                    <div class="alert alert-warning">
                        <strong>No ad groups found</strong><br>
                        Searched ${data.customers_found} Beslist.nl accounts.
                    </div>
                `;
            } else {
                const jobsList = data.job_ids ? data.job_ids.join(', ') : 'N/A';
                const multipleJobs = data.jobs_created > 1;

                resultDiv.innerHTML = `
                    <div class="alert alert-success">
                        <strong>Success!</strong> ${multipleJobs ? data.jobs_created + ' jobs' : 'Job ' + data.job_ids[0]} created.<br>
                        Found ${data.ad_groups_discovered} ad groups to process${multipleJobs ? ` (split into ${data.jobs_created} jobs of ~${Math.ceil(data.total_items / data.jobs_created)} items each)` : ''}.<br>
                        ${multipleJobs ? 'Job IDs: ' + jobsList : ''}
                        Processing started automatically.
                    </div>
                `;
                // Start polling for all jobs
                if (data.job_ids && data.job_ids.length > 0) {
                    currentJobId = data.job_ids[0];
                    startPolling(data.job_ids[0]);
                }
            }
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
    }
}

async function refreshJobs() {
    try {
        const response = await fetch('/api/thema-ads/jobs?limit=20');
        const data = await response.json();

        const jobsList = document.getElementById('jobsList');

        if (!data.jobs || data.jobs.length === 0) {
            jobsList.innerHTML = '<p class="text-muted">No jobs yet. Upload a CSV or use Auto-Discover to create a job.</p>';
            return;
        }

        let html = '<table class="table table-sm">';
        html += '<thead><tr><th>Job ID</th><th>Theme</th><th>Status</th><th>Progress</th><th>Success</th><th>Failed</th><th>Skipped</th><th>Actions</th></tr></thead><tbody>';

        for (const job of data.jobs) {
            const progress = job.total_items > 0
                ? Math.round((job.successful_items + job.failed_items + job.skipped_items) / job.total_items * 100)
                : 0;

            const statusBadge = getStatusBadge(job.status);

            // Get theme display name
            const theme = themes.find(t => t.name === job.theme_name);
            const themeDisplay = theme ? theme.display_name : (job.theme_name || 'Singles Day');

            html += `
                <tr>
                    <td><a href="#" onclick="viewJob(${job.id}); return false;">#${job.id}</a></td>
                    <td><span class="badge bg-secondary">${themeDisplay}</span></td>
                    <td>${statusBadge}</td>
                    <td>${progress}%</td>
                    <td class="text-success">${job.successful_items}</td>
                    <td class="text-danger">${job.failed_items}</td>
                    <td class="text-info">${job.skipped_items}</td>
                    <td>
                        ${job.status === 'pending' ? `<button class="btn btn-sm btn-success" onclick="startJobById(${job.id})">Start</button>` : ''}
                        ${job.status === 'running' ? `<button class="btn btn-sm btn-warning" onclick="pauseJobById(${job.id})">Pause</button>` : ''}
                        ${job.status === 'paused' || job.status === 'failed' ? `<button class="btn btn-sm btn-info" onclick="resumeJobById(${job.id})">Resume</button>` : ''}
                        ${job.is_repair_job && (job.status === 'completed' || job.status === 'failed' || job.status === 'running') ? `<button class="btn btn-sm btn-warning" onclick="labelCheckupFailed(${job.id})" title="Mark these ad groups to exclude from future checkups">Label Failed</button>` : ''}
                        ${job.status === 'completed' || job.status === 'paused' ? `<button class="btn btn-sm btn-danger" onclick="deleteJobById(${job.id})">Delete</button>` : ''}
                        <a href="/api/thema-ads/jobs/${job.id}/plan-csv" class="btn btn-sm btn-primary" title="Download uploaded plan">Plan CSV</a>
                        ${job.successful_items > 0 ? `<a href="/api/thema-ads/jobs/${job.id}/successful-items-csv" class="btn btn-sm btn-success" title="Download successful items">Success CSV</a>` : ''}
                        ${(job.failed_items > 0 || job.skipped_items > 0) ? `<a href="/api/thema-ads/jobs/${job.id}/failed-items-csv" class="btn btn-sm btn-secondary" title="Download failed/skipped items">Failed CSV</a>` : ''}
                    </td>
                </tr>
            `;
        }

        html += '</tbody></table>';
        jobsList.innerHTML = html;

    } catch (error) {
        console.error('Error refreshing jobs:', error);
    }
}

function getStatusBadge(status) {
    const badges = {
        'pending': '<span class="badge bg-secondary">Pending</span>',
        'running': '<span class="badge bg-primary">Running</span>',
        'paused': '<span class="badge bg-warning">Paused</span>',
        'completed': '<span class="badge bg-success">Completed</span>',
        'failed': '<span class="badge bg-danger">Failed</span>'
    };
    return badges[status] || status;
}

async function viewJob(jobId) {
    currentJobId = jobId;
    startPolling(jobId);
}

async function startJobById(jobId) {
    try {
        const response = await fetch(`/api/thema-ads/jobs/${jobId}/start`, { method: 'POST' });
        const data = await response.json();
        if (response.ok) {
            viewJob(jobId);
        }
    } catch (error) {
        alert('Error starting job: ' + error.message);
    }
}

async function pauseJobById(jobId) {
    try {
        await fetch(`/api/thema-ads/jobs/${jobId}/pause`, { method: 'POST' });
        refreshJobs();
    } catch (error) {
        alert('Error pausing job: ' + error.message);
    }
}

async function resumeJobById(jobId) {
    try {
        const response = await fetch(`/api/thema-ads/jobs/${jobId}/resume`, { method: 'POST' });
        if (response.ok) {
            viewJob(jobId);
        }
    } catch (error) {
        alert('Error resuming job: ' + error.message);
    }
}

async function deleteJobById(jobId) {
    if (!confirm('Are you sure you want to delete this job?')) return;

    try {
        await fetch(`/api/thema-ads/jobs/${jobId}`, { method: 'DELETE' });
        refreshJobs();
        if (currentJobId === jobId) {
            stopPolling();
            document.getElementById('currentJobCard').style.display = 'none';
        }
    } catch (error) {
        alert('Error deleting job: ' + error.message);
    }
}

async function labelCheckupFailed(jobId) {
    if (!confirm('This will label all ad groups in this repair job with THEMES_CHECKUP_FAILED to exclude them from future checkup runs. Continue?')) {
        return;
    }

    try {
        const formData = new FormData();
        formData.append('job_ids', jobId);

        const response = await fetch('/api/thema-ads/label-checkup-failed', {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (!response.ok) {
            alert('Error: ' + (result.detail || 'Unknown error'));
            return;
        }

        alert(`Success!\n\nLabel: ${result.label_applied}\nAd groups found: ${result.total_ad_groups_found}\nAd groups labeled: ${result.total_labeled}\nCustomers processed: ${result.customers_processed}`);
        refreshJobs();
    } catch (error) {
        alert('Error labeling ad groups: ' + error.message);
    }
}

function startPolling(jobId) {
    stopPolling();
    pollInterval = setInterval(() => updateJobStatus(jobId), 2000);
    updateJobStatus(jobId);
    document.getElementById('currentJobCard').style.display = 'block';
}

function stopPolling() {
    if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
    }
}

async function updateJobStatus(jobId) {
    try {
        const response = await fetch(`/api/thema-ads/jobs/${jobId}`);
        const job = await response.json();

        if (!response.ok) {
            stopPolling();
            return;
        }

        // Update UI
        document.getElementById('currentJobId').textContent = job.id;
        document.getElementById('jobStatus').textContent = job.status;
        document.getElementById('jobStatus').className = 'badge ' + getStatusClass(job.status);
        document.getElementById('jobStarted').textContent = formatDateTime(job.started_at);

        // Update counts
        document.getElementById('totalItems').textContent = job.total_items;
        document.getElementById('successfulItems').textContent = job.successful_items;
        document.getElementById('skippedItems').textContent = job.skipped_items;
        document.getElementById('failedItems').textContent = job.failed_items;
        document.getElementById('pendingItems').textContent = job.pending_items;

        // Update progress bar
        const processed = job.successful_items + job.failed_items + job.skipped_items;
        const progress = job.total_items > 0 ? Math.round(processed / job.total_items * 100) : 0;
        document.getElementById('progressBar').style.width = progress + '%';
        document.getElementById('progressBar').textContent = progress + '%';
        document.getElementById('progressText').textContent = `${processed} / ${job.total_items}`;

        // Update buttons
        const startBtn = document.getElementById('startBtn');
        const pauseBtn = document.getElementById('pauseBtn');
        const resumeBtn = document.getElementById('resumeBtn');

        startBtn.style.display = job.status === 'pending' ? 'inline-block' : 'none';
        pauseBtn.style.display = job.status === 'running' ? 'inline-block' : 'none';
        resumeBtn.style.display = (job.status === 'paused' || job.status === 'failed') ? 'inline-block' : 'none';

        // Stop polling if job is done
        if (job.status === 'completed' || job.status === 'paused') {
            stopPolling();
        }

        // Refresh job list
        refreshJobs();

    } catch (error) {
        console.error('Error updating job status:', error);
    }
}

function getStatusClass(status) {
    const classes = {
        'pending': 'bg-secondary',
        'running': 'bg-primary',
        'paused': 'bg-warning',
        'completed': 'bg-success',
        'failed': 'bg-danger'
    };
    return classes[status] || 'bg-secondary';
}

async function startJob() {
    if (!currentJobId) return;
    await startJobById(currentJobId);
}

async function pauseJob() {
    if (!currentJobId) return;
    await pauseJobById(currentJobId);
}

async function resumeJob() {
    if (!currentJobId) return;
    await resumeJobById(currentJobId);
}

async function runCheckup() {
    const limit = document.getElementById('checkupLimit').value;
    const batchSize = document.getElementById('checkupBatchSize').value;
    const jobChunkSize = document.getElementById('checkupJobChunkSize').value;
    const skipAudited = document.getElementById('checkupSkipAudited').checked;
    const resultDiv = document.getElementById('checkupResult');
    const btn = document.getElementById('checkupBtn');

    btn.disabled = true;

    // Show enhanced progress display with spinner and progress bar
    resultDiv.innerHTML = `
        <div class="alert alert-info">
            <div class="d-flex align-items-center mb-3">
                <div class="spinner-border spinner-border-sm me-2" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <strong>Running optimized check-up...</strong>
            </div>
            <div class="progress mb-3" style="height: 25px;">
                <div class="progress-bar progress-bar-striped progress-bar-animated bg-info"
                     role="progressbar"
                     style="width: 100%">
                    Auditing ad groups and verifying theme ads...
                </div>
            </div>
            <small class="text-muted">
                <strong>Configuration:</strong>
                ${skipAudited ? 'Skip previously audited' : 'Check all ad groups'}
                ${limit ? ` | Limit: ${limit} ad groups` : ' | No limit'}
                | Batch size: ${batchSize}
                <br>
                <em>Note: Progress updates will be shown after completion. The system is processing customers in parallel.</em>
            </small>
        </div>
    `;

    try {
        const params = new URLSearchParams();
        if (limit) params.append('limit', limit);
        params.append('batch_size', batchSize);
        params.append('job_chunk_size', jobChunkSize);
        params.append('skip_audited', skipAudited);

        const response = await fetch(`/api/thema-ads/checkup?${params}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (response.ok) {
            const stats = data.stats;
            const repairJobsList = data.repair_job_ids && data.repair_job_ids.length > 0
                ? data.repair_job_ids.join(', ')
                : 'None';

            resultDiv.innerHTML = `
                <div class="alert alert-success">
                    <strong>Check-up completed!</strong><br>
                    <hr>
                    <div class="row text-center">
                        <div class="col-md-3">
                            <strong>Customers Processed</strong><br>
                            <span class="badge bg-primary fs-6">${stats.customers_processed}</span>
                        </div>
                        <div class="col-md-3">
                            <strong>Ad Groups Checked</strong><br>
                            <span class="badge bg-info fs-6">${stats.ad_groups_checked}</span>
                        </div>
                        <div class="col-md-3">
                            <strong>Verified (OK)</strong><br>
                            <span class="badge bg-success fs-6">${stats.ad_groups_verified}</span>
                        </div>
                        <div class="col-md-3">
                            <strong>Missing Theme Ads</strong><br>
                            <span class="badge bg-warning fs-6">${stats.ad_groups_missing_theme_ad || stats.ad_groups_missing_singles_day || 0}</span>
                        </div>
                    </div>
                    <hr>
                    <strong>DONE labels removed:</strong> ${stats.done_labels_removed || 0}<br>
                    <strong>THEMES_CHECK_DONE labels applied:</strong> ${stats.themes_check_done_labels_applied || stats.sd_checked_labels_applied || 0}<br>
                    <strong>Repair jobs created:</strong> ${stats.repair_jobs_created || 0}
                    ${data.repair_job_ids && data.repair_job_ids.length > 0 ? '<br><strong>Repair Job IDs:</strong> ' + repairJobsList : ''}
                </div>
            `;

            // Start polling for first repair job if any were created
            if (data.repair_job_ids && data.repair_job_ids.length > 0) {
                currentJobId = data.repair_job_ids[0];
                startPolling(data.repair_job_ids[0]);
            }
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
    }
}

async function runCleanup() {
    const dryRun = document.getElementById('cleanupDryRun').checked;
    const resultDiv = document.getElementById('cleanupResult');
    const btn = document.getElementById('cleanupBtn');

    btn.disabled = true;

    // Show progress
    resultDiv.innerHTML = `
        <div class="alert alert-info">
            <div class="d-flex align-items-center mb-3">
                <div class="spinner-border spinner-border-sm me-2" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <strong>${dryRun ? 'Analyzing' : 'Cleaning up'} conflicting labels across all 28 customers...</strong>
            </div>
            <div class="progress mb-3" style="height: 25px;">
                <div class="progress-bar progress-bar-striped progress-bar-animated ${dryRun ? 'bg-info' : 'bg-warning'}"
                     role="progressbar"
                     style="width: 100%">
                    ${dryRun ? 'Scanning for conflicting labels...' : 'Removing THEMA_ORIGINAL labels...'}
                </div>
            </div>
            <small class="text-muted">
                <strong>Mode:</strong> ${dryRun ? 'DRY RUN (preview only)' : 'LIVE (making changes)'}
                <br>
                <em>This may take 2-3 minutes to complete...</em>
            </small>
        </div>
    `;

    try {
        const response = await fetch(`/api/thema-ads/cleanup-thema-original?dry_run=${dryRun}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (response.ok) {
            const alertType = dryRun ? 'info' : 'success';
            const icon = dryRun ? 'bi-info-circle' : 'bi-check-circle';
            const actionText = dryRun ? 'Would fix' : 'Fixed';

            resultDiv.innerHTML = `
                <div class="alert alert-${alertType}">
                    <h5><i class="bi ${icon}"></i> ${dryRun ? 'Preview Complete' : 'Cleanup Complete!'}</h5>
                    <hr>
                    <div class="row text-center">
                        <div class="col-md-4">
                            <strong>Ads with Conflicts</strong><br>
                            <span class="badge bg-warning fs-5">${data.total_checked.toLocaleString()}</span>
                        </div>
                        <div class="col-md-4">
                            <strong>${actionText}</strong><br>
                            <span class="badge ${dryRun ? 'bg-info' : 'bg-success'} fs-5">${data.total_fixed.toLocaleString()}</span>
                        </div>
                        <div class="col-md-4">
                            <strong>Failed</strong><br>
                            <span class="badge ${data.total_failed > 0 ? 'bg-danger' : 'bg-secondary'} fs-5">${data.total_failed}</span>
                        </div>
                    </div>
                    <hr>
                    <p class="mb-0">
                        ${dryRun
                            ? `<strong><i class="bi bi-exclamation-triangle"></i> Preview Mode:</strong>
                               No changes were made. Uncheck "Dry-run mode" and click "Run Cleanup" again to execute.`
                            : `<strong><i class="bi bi-check-circle"></i> Success:</strong>
                               Removed THEMA_ORIGINAL labels from ${data.total_fixed.toLocaleString()} ads that have theme labels.`
                        }
                    </p>
                </div>
            `;
        } else {
            resultDiv.innerHTML = `
                <div class="alert alert-danger">
                    <strong>Error:</strong> ${data.detail || 'Cleanup failed'}
                </div>
            `;
        }
    } catch (error) {
        resultDiv.innerHTML = `
            <div class="alert alert-danger">
                <strong>Error:</strong> ${error.message}
            </div>
        `;
    } finally {
        btn.disabled = false;
    }
}

async function removeCheckupLabels() {
    const resultDiv = document.getElementById('checkupResult');
    const btn = document.getElementById('removeLabelsBtn');

    if (!confirm('Are you sure you want to remove all THEMES_CHECK_DONE labels? This will allow re-auditing all ad groups from scratch.')) {
        return;
    }

    btn.disabled = true;
    resultDiv.innerHTML = '<div class="alert alert-info">Removing THEMES_CHECK_DONE labels...</div>';

    try {
        const response = await fetch('/api/thema-ads/remove-checkup-labels', {
            method: 'POST'
        });

        const data = await response.json();

        if (response.ok) {
            const stats = data.stats;

            resultDiv.innerHTML = `
                <div class="alert alert-success">
                    <strong>Audit labels removed successfully!</strong><br>
                    <hr>
                    <div class="row text-center">
                        <div class="col-md-4">
                            <strong>Customers Processed</strong><br>
                            <span class="badge bg-primary fs-6">${stats.customers_processed}</span>
                        </div>
                        <div class="col-md-4">
                            <strong>Ad Groups Found</strong><br>
                            <span class="badge bg-info fs-6">${stats.ad_groups_with_check_done_label}</span>
                        </div>
                        <div class="col-md-4">
                            <strong>Labels Removed</strong><br>
                            <span class="badge bg-success fs-6">${stats.check_done_labels_removed}</span>
                        </div>
                    </div>
                    <hr>
                    <p class="mb-0">You can now run a clean audit to re-check all ad groups.</p>
                </div>
            `;
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
    }
}


// Auto-Queue Management
async function loadQueueStatus() {
    try {
        const response = await fetch('/api/thema-ads/queue/status');
        const data = await response.json();

        if (response.ok) {
            const enabled = data.auto_queue_enabled;
            const toggle = document.getElementById('autoQueueToggle');
            const statusText = document.getElementById('queueStatusText');

            // Update toggle
            if (toggle) {
                toggle.checked = enabled;
            }

            // Update status text
            if (statusText) {
                if (enabled) {
                    statusText.innerHTML = '<span class="text-success">Enabled - Jobs will start automatically after current job completes (30s delay)</span>';
                } else {
                    statusText.innerHTML = '<span class="text-muted">Disabled - Jobs must be started manually</span>';
                }
            }
        }
    } catch (error) {
        console.error('Error loading queue status:', error);
    }
}

async function toggleAutoQueue() {
    const toggle = document.getElementById('autoQueueToggle');
    const enabled = toggle.checked;

    try {
        const endpoint = enabled ? '/api/thema-ads/queue/enable' : '/api/thema-ads/queue/disable';
        const response = await fetch(endpoint, { method: 'POST' });
        const data = await response.json();

        if (response.ok) {
            // Reload status to update UI
            await loadQueueStatus();

            // Show notification
            const statusText = document.getElementById('queueStatusText');
            if (statusText) {
                const color = enabled ? 'success' : 'warning';
                const message = enabled ? 'Auto-queue enabled!' : 'Auto-queue disabled';
                statusText.innerHTML = `<span class="text-${color}"><strong>${message}</strong></span>`;

                // Reload after 2 seconds to show normal status
                setTimeout(loadQueueStatus, 2000);
            }
        } else {
            // Revert toggle on error
            toggle.checked = !enabled;
            alert(`Failed to ${enabled ? 'enable' : 'disable'} auto-queue: ${data.detail}`);
        }
    } catch (error) {
        // Revert toggle on error
        toggle.checked = !enabled;
        alert(`Error: ${error.message}`);
    }
}

async function uploadActivationPlan() {
    const fileInput = document.getElementById('activationPlanFile');
    const resetLabels = document.getElementById('resetActivationLabels').checked;
    const resultDiv = document.getElementById('uploadPlanResult');
    const btn = document.getElementById('uploadPlanBtn');

    if (!fileInput.files.length) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please select an Excel file</div>';
        return;
    }

    btn.disabled = true;
    resultDiv.innerHTML = '<div class="alert alert-info">Uploading activation plan...</div>';

    try {
        const formData = new FormData();
        formData.append('file', fileInput.files[0]);
        formData.append('is_activation_plan', 'true');
        formData.append('reset_activation_labels', resetLabels ? 'true' : 'false');

        const response = await fetch('/api/thema-ads/upload-excel', {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (response.ok) {
            let resultHTML = '<div class="alert alert-success">';
            resultHTML += `<h5>${data.message}</h5>`;
            resultHTML += `<strong>Customers in plan:</strong> ${data.customers_in_plan}<br>`;
            if (data.reset_labels) {
                resultHTML += '<strong>Activation labels reset:</strong> Yes<br>';
            }
            resultHTML += '</div>';
            resultDiv.innerHTML = resultHTML;

            // Reload current plan
            await loadActivationPlan();
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
    }
}

async function loadActivationPlan() {
    const planDiv = document.getElementById('currentPlan');
    if (!planDiv) return;

    try {
        const response = await fetch('/api/thema-ads/activation-plan');
        const data = await response.json();

        if (response.ok && data.customer_count > 0) {
            let html = `<p><strong>${data.customer_count} customers in plan</strong></p>`;
            html += '<div class="table-responsive" style="max-height: 300px; overflow-y: auto;">';
            html += '<table class="table table-sm">';
            html += '<thead><tr><th>Customer ID</th><th>Theme</th></tr></thead>';
            html += '<tbody>';
            for (const [customerId, theme] of Object.entries(data.plan)) {
                const themeName = themes.find(t => t.name === theme)?.display_name || theme;
                html += `<tr><td>${customerId}</td><td>${themeName}</td></tr>`;
            }
            html += '</tbody></table></div>';
            planDiv.innerHTML = html;
        } else {
            planDiv.innerHTML = '<p class="text-muted">No activation plan uploaded yet. Upload an Excel file with customer_id and theme columns.</p>';
        }
    } catch (error) {
        planDiv.innerHTML = '<p class="text-danger">Error loading plan</p>';
    }
}

async function activateAds() {
    const customerIdsInput = document.getElementById('activateCustomerIds').value;
    const resetLabels = document.getElementById('activateResetLabels').checked;
    const parallelWorkers = parseInt(document.getElementById('activateParallelWorkers').value) || 5;
    const resultDiv = document.getElementById('activateResult');
    const btn = document.getElementById('activateAdsBtn');

    btn.disabled = true;
    resultDiv.innerHTML = `
        <div class="alert alert-info">
            <strong>Activating ads (optimized)...</strong><br>
            Processing ${parallelWorkers} customers in parallel for maximum speed.<br>
            <div class="mt-3">
                <strong>Progress:</strong> <span id="activationProgress">Starting...</span><br>
                <div class="progress mt-2" style="height: 25px;">
                    <div id="activationProgressBar" class="progress-bar progress-bar-striped progress-bar-animated"
                         role="progressbar" style="width: 0%">0%</div>
                </div>
            </div>
        </div>
    `;

    // Start progress polling
    let pollInterval = null;
    let lastJobsSnapshot = null;

    const updateProgress = async () => {
        try {
            const jobsResponse = await fetch('/api/thema-ads/jobs?limit=50');
            const jobsData = await jobsResponse.json();
            const runningJobs = jobsData.jobs.filter(j => j.status === 'running' || j.status === 'pending');

            if (runningJobs.length > 0) {
                // Calculate total progress across all running jobs
                let totalAds = 0;
                let processedAds = 0;

                runningJobs.forEach(job => {
                    totalAds += job.total_ad_groups || 0;
                    processedAds += job.processed_ad_groups || 0;
                });

                const progressPct = totalAds > 0 ? Math.round((processedAds / totalAds) * 100) : 0;

                // Update progress display
                document.getElementById('activationProgress').textContent =
                    `${processedAds.toLocaleString()} / ${totalAds.toLocaleString()} ad groups processed`;
                document.getElementById('activationProgressBar').style.width = `${progressPct}%`;
                document.getElementById('activationProgressBar').textContent = `${progressPct}%`;

                lastJobsSnapshot = runningJobs;
            }
        } catch (e) {
            console.error('Failed to update progress:', e);
        }
    };

    // Poll every 2 seconds
    pollInterval = setInterval(updateProgress, 2000);

    try {
        // Parse customer IDs if provided
        let customerIds = null;
        if (customerIdsInput.trim()) {
            customerIds = customerIdsInput.split(',').map(id => id.trim()).filter(id => id);
        }

        // Build query parameters
        const params = new URLSearchParams();
        if (resetLabels) params.append('reset_labels', 'true');
        params.append('parallel_workers', parallelWorkers);
        if (customerIds) {
            customerIds.forEach(id => params.append('customer_ids', id));
        }

        const response = await fetch(`/api/thema-ads/activate-v2?${params.toString()}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (response.ok && data.status === 'accepted') {
            // New BackgroundTasks pattern - activation running in background
            resultDiv.innerHTML = `
                <div class="alert alert-info">
                    <h5><i class="fas fa-spinner fa-spin"></i> Activation Started!</h5>
                    <hr>
                    <p>${data.message}</p>
                    <p><strong>Processing ${customerIds ? customerIds.length : 'all'} customers with ${parallelWorkers} parallel workers.</strong></p>
                    <p class="mb-0"><small>The activation is running in the background. Check the logs with: <code>docker logs theme_ads-app-1 --tail 100</code></small></p>
                </div>
            `;
        } else if (response.ok && data.status === 'completed') {
            // Legacy pattern - immediate completion (fallback)
            const stats = data.stats;

            let resultHTML = '<div class="alert alert-success">';
            resultHTML += '<h5>Ad Activation Completed!</h5>';
            resultHTML += '<hr>';
            resultHTML += `<strong>Customers Processed:</strong> ${stats.customers_processed}<br>`;
            resultHTML += `<strong>Ad Groups Checked:</strong> ${stats.ad_groups_checked}<br>`;
            resultHTML += `<strong>Ad Groups Activated:</strong> ${stats.ad_groups_activated}<br>`;
            resultHTML += `<strong>Already Correct:</strong> ${stats.ad_groups_already_correct}<br>`;
            resultHTML += `<strong>Skipped (Done Label):</strong> ${stats.ad_groups_skipped_done_label}<br>`;
            resultHTML += `<strong>Missing Theme Ad:</strong> ${stats.ad_groups_missing_theme_ad}<br>`;
            resultHTML += '</div>';

            // Show missing ads if any
            if (stats.ad_groups_missing_theme_ad > 0) {
                resultHTML += '<div class="alert alert-warning mt-2">';
                resultHTML += `<strong>${stats.ad_groups_missing_theme_ad} ad groups are missing the required theme ad.</strong><br>`;
                resultHTML += 'Check the "Missing Ads" section below to download a CSV and add the missing theme ads.';
                resultHTML += '</div>';

                // Load and display missing ads
                await loadMissingAds();
            }

            resultDiv.innerHTML = resultHTML;
        } else if (data.status === 'error') {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.message}</div>`;
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail || 'Unknown error'}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        // Stop polling
        if (pollInterval) {
            clearInterval(pollInterval);
        }
        btn.disabled = false;
    }
}

async function loadMissingAds() {
    try {
        const response = await fetch('/api/thema-ads/activation-missing-ads');
        const data = await response.json();

        if (response.ok && data.count > 0) {
            const missingAdsCard = document.getElementById('missingAdsCard');
            const tableBody = document.getElementById('missingAdsTableBody');

            // Show card
            missingAdsCard.style.display = 'block';

            // Populate table
            tableBody.innerHTML = '';
            data.missing_ads.forEach(ad => {
                const themeName = themes.find(t => t.name === ad.required_theme)?.display_name || ad.required_theme;
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${ad.customer_id}</td>
                    <td>${ad.campaign_name || ad.campaign_id}</td>
                    <td>${ad.ad_group_name || ad.ad_group_id}</td>
                    <td>${themeName}</td>
                `;
                tableBody.appendChild(row);
            });
        }
    } catch (error) {
        console.error('Error loading missing ads:', error);
    }
}

function downloadMissingAds() {
    window.open('/api/thema-ads/activation-missing-ads/export', '_blank');
}

async function runAllThemes() {
    const customerFilter = document.getElementById('allThemesCustomerFilter').value;
    const limit = document.getElementById('allThemesLimit').value || null;
    const batchSize = document.getElementById('allThemesBatchSize').value;
    const jobChunkSize = document.getElementById('allThemesJobChunkSize').value;
    const resultDiv = document.getElementById('allThemesResult');
    const btn = document.getElementById('runAllThemesBtn');

    // Get selected themes
    const checkboxes = document.querySelectorAll('.all-themes-checkbox:checked');
    const selectedThemes = Array.from(checkboxes).map(cb => cb.value);

    if (selectedThemes.length === 0) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please select at least one theme to process</div>';
        return;
    }

    if (!customerFilter.trim()) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please enter a customer filter</div>';
        return;
    }

    btn.disabled = true;
    resultDiv.innerHTML = `
        <div class="alert alert-info">
            <strong>Running all-themes discovery...</strong><br>
            Customer Filter: ${customerFilter}<br>
            Selected Themes: ${selectedThemes.map(t => themes.find(th => th.name === t)?.display_name || t).join(', ')}<br>
            Limit: ${limit || 'No limit'}<br>
            This may take a few minutes...
        </div>
    `;

    try {
        // Build query parameters
        const params = new URLSearchParams();
        params.append('customer_filter', customerFilter);
        if (limit) params.append('limit', limit);
        params.append('batch_size', batchSize);
        params.append('job_chunk_size', jobChunkSize);

        // Add themes as array
        selectedThemes.forEach(theme => params.append('themes', theme));

        const response = await fetch(`/api/thema-ads/run-all-themes?${params.toString()}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (response.ok) {
            const stats = data.stats;
            const jobIdsByTheme = data.job_ids_by_theme;

            let resultHTML = '<div class="alert alert-success">';
            resultHTML += '<h5>All-Themes Discovery Completed!</h5>';
            resultHTML += '<hr>';
            resultHTML += `<strong>Customers Found:</strong> ${stats.customers_found}<br>`;
            resultHTML += `<strong>Customers Processed:</strong> ${stats.customers_processed}<br>`;
            resultHTML += `<strong>Ad Groups Analyzed:</strong> ${stats.ad_groups_analyzed}<br>`;
            resultHTML += `<strong>Ad Groups with Missing Themes:</strong> ${stats.ad_groups_with_missing_themes}<br>`;
            resultHTML += '<hr>';
            resultHTML += '<strong>Missing Themes Breakdown:</strong><ul>';
            for (const [theme, count] of Object.entries(stats.missing_by_theme)) {
                const themeName = themes.find(t => t.name === theme)?.display_name || theme;
                resultHTML += `<li>${themeName}: ${count} ad groups</li>`;
            }
            resultHTML += '</ul>';

            // Show jobs created
            if (Object.keys(jobIdsByTheme).length > 0) {
                resultHTML += '<hr><strong>Jobs Created:</strong><ul>';
                for (const [theme, jobIds] of Object.entries(jobIdsByTheme)) {
                    const themeName = themes.find(t => t.name === theme)?.display_name || theme;
                    resultHTML += `<li>${themeName}: ${jobIds.length} job(s) (IDs: ${jobIds.join(', ')})</li>`;
                }
                resultHTML += '</ul>';
                resultHTML += '<p class="mt-2"><strong>Jobs have been created and will be processed automatically!</strong></p>';
            } else {
                resultHTML += '<hr><p><strong>No jobs created - all ad groups already have the selected themes!</strong></p>';
            }

            resultHTML += '</div>';
            resultDiv.innerHTML = resultHTML;

            // Refresh job list
            refreshJobs();
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
    }
}

// Remove Duplicates function
async function removeDuplicates() {
    const customerFilter = document.getElementById('duplicateCustomerFilter').value;
    const singleCustomer = document.getElementById('duplicateSingleCustomer').value.trim();
    const limit = document.getElementById('duplicateLimit').value;
    const dryRun = document.getElementById('duplicateDryRun').checked;
    const resetLabels = document.getElementById('duplicateResetLabels').checked;
    const resultDiv = document.getElementById('removeDuplicatesResult');
    const btn = document.getElementById('removeDuplicatesBtn');

    // Validate single customer input if needed
    if (customerFilter === 'single' && !singleCustomer) {
        resultDiv.innerHTML = '<div class="alert alert-danger">Please enter a customer ID</div>';
        return;
    }

    btn.disabled = true;

    const modeText = dryRun ? '(DRY RUN - Preview Only)' : '(LIVE - Will Remove Ads)';
    const modeClass = dryRun ? 'alert-info' : 'alert-warning';

    resultDiv.innerHTML = `
        <div class="alert ${modeClass}">
            <strong>Finding duplicate ads ${modeText}</strong><br>
            Scanning ad groups for duplicate content...<br>
            This may take a few minutes depending on the number of ad groups.<br>
            Please wait...
        </div>
    `;

    try {
        // Build query parameters
        const params = new URLSearchParams();
        params.append('dry_run', dryRun);
        params.append('reset_labels', resetLabels);

        if (limit) {
            params.append('limit', limit);
        }

        // Handle customer IDs based on filter
        let customerIds = null;
        if (customerFilter === 'single') {
            customerIds = [singleCustomer];
        } else if (customerFilter === 'beslist') {
            // Backend will use default Beslist.nl customers
            customerIds = null;
        } else {
            // all - backend will get all customers
            customerIds = null;
        }

        if (customerIds) {
            customerIds.forEach(id => params.append('customer_ids', id));
        }

        const response = await fetch(`/api/thema-ads/remove-duplicates?${params.toString()}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (response.ok && data.status === 'completed') {
            const stats = data.stats;
            const isDryRun = data.dry_run;

            let resultHTML = `<div class="alert ${isDryRun ? 'alert-info' : 'alert-success'}">`;
            resultHTML += `<h5>${isDryRun ? 'Duplicate Analysis Completed (DRY RUN)' : 'Duplicate Removal Completed!'}</h5>`;
            resultHTML += '<hr>';
            resultHTML += `<strong>Customers Processed:</strong> ${stats.customers_processed}<br>`;
            resultHTML += `<strong>Ad Groups Checked:</strong> ${stats.ad_groups_checked}<br>`;
            resultHTML += `<strong>Ad Groups with Duplicates:</strong> ${stats.ad_groups_with_duplicates}<br>`;
            resultHTML += `<strong>Duplicate Sets Found:</strong> ${stats.duplicate_sets_found}<br>`;
            resultHTML += `<strong>Ads ${isDryRun ? 'That Would Be Removed' : 'Removed'}:</strong> ${stats.ads_removed}<br>`;

            if (isDryRun) {
                resultHTML += '<hr>';
                resultHTML += '<div class="alert alert-warning mb-0">';
                resultHTML += '<strong>This was a DRY RUN.</strong> No ads were actually removed.<br>';
                resultHTML += 'Uncheck "Dry Run" and run again to actually remove the duplicate ads.';
                resultHTML += '</div>';
            } else {
                resultHTML += '<hr>';
                resultHTML += '<div class="alert alert-success mb-0">';
                resultHTML += `<strong>Successfully removed ${stats.ads_removed} duplicate ads!</strong><br>`;
                resultHTML += `Labeled ${stats.ad_groups_with_duplicates} ad groups with THEME_DUPLICATES_CHECK to prevent reprocessing.`;
                resultHTML += '</div>';
            }

            resultHTML += '</div>';
            resultDiv.innerHTML = resultHTML;
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail || 'Unknown error'}</div>`;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
    }
}

// Event listener for duplicate customer filter
document.getElementById('duplicateCustomerFilter')?.addEventListener('change', function() {
    const singleCustomerDiv = document.getElementById('duplicateSingleCustomerDiv');
    if (this.value === 'single') {
        singleCustomerDiv.style.display = 'block';
    } else {
        singleCustomerDiv.style.display = 'none';
    }
});
