// Vanilla JavaScript - no build tools, no transpilation!

// Use dynamic API base - works from localhost, WSL IP, or any host
const API_BASE = window.location.origin;

// Check system status on load
window.addEventListener('DOMContentLoaded', () => {
    checkStatus();
    refreshStatus();

    // Handle conservative mode checkbox for SEO content generation
    const conservativeCheckbox = document.getElementById('conservativeModeCheckbox');
    const parallelWorkersInput = document.getElementById('parallelWorkersInput');

    conservativeCheckbox.addEventListener('change', function() {
        if (this.checked) {
            parallelWorkersInput.value = 1;
            parallelWorkersInput.disabled = true;
        } else {
            parallelWorkersInput.disabled = false;
        }
    });

    // Handle conservative mode checkbox for link validation
    const validationConservativeCheckbox = document.getElementById('validationConservativeModeCheckbox');
    const validationParallelWorkersInput = document.getElementById('validationParallelWorkers');

    validationConservativeCheckbox.addEventListener('change', function() {
        if (this.checked) {
            validationParallelWorkersInput.value = 1;
            validationParallelWorkersInput.disabled = true;
        } else {
            validationParallelWorkersInput.disabled = false;
        }
    });
});

async function checkStatus() {
    try {
        const response = await fetch(`${API_BASE}/`);
        const data = await response.json();
        document.getElementById('status').innerHTML = `
            <span class="status-ok">✓ Connected</span> |
            Project: ${data.project} |
            Server Time: ${new Date(data.timestamp).toLocaleTimeString()}
        `;
    } catch (error) {
        document.getElementById('status').innerHTML =
            '<span class="status-error">✗ Cannot connect to backend</span>';
    }
}

async function testAPI() {
    const prompt = document.getElementById('promptInput').value;
    const resultDiv = document.getElementById('result');

    if (!prompt) {
        alert('Please enter a prompt');
        return;
    }

    resultDiv.textContent = 'Thinking...';
    resultDiv.classList.add('show');

    try {
        const response = await fetch(`${API_BASE}/api/generate`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ prompt })
        });

        const data = await response.json();
        resultDiv.textContent = data.response || 'No response';
    } catch (error) {
        resultDiv.textContent = `Error: ${error.message}`;
    }
}

let processingActive = false;
let shouldStop = false;

async function processUrls() {
    const btn = document.getElementById('processBtn');
    const resultDiv = document.getElementById('processResult');
    const batchSizeInput = document.getElementById('batchSizeInput');
    const parallelWorkersInput = document.getElementById('parallelWorkersInput');
    const conservativeModeCheckbox = document.getElementById('conservativeModeCheckbox');
    const batchSize = parseInt(batchSizeInput.value) || 10;
    const parallelWorkers = parseInt(parallelWorkersInput.value) || 1;
    const conservativeMode = conservativeModeCheckbox.checked;

    if (batchSize < 1) {
        alert('Batch size must be at least 1');
        return;
    }

    if (parallelWorkers < 1 || parallelWorkers > 20) {
        alert('Parallel workers must be between 1 and 20');
        return;
    }

    btn.disabled = true;
    btn.textContent = 'Processing...';
    const modeText = conservativeMode ? ' (Conservative Mode)' : '';
    resultDiv.innerHTML = `<div class="alert alert-warning">Processing ${batchSize} URL(s) with ${parallelWorkers} parallel worker(s)${modeText}...</div>`;

    try {
        const response = await fetch(`${API_BASE}/api/process-urls?batch_size=${batchSize}&parallel_workers=${parallelWorkers}&conservative_mode=${conservativeMode}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (data.status === 'complete' && data.processed === 0) {
            resultDiv.innerHTML = `
                <div class="alert alert-warning">
                    <strong>No URLs to process</strong><br>
                    ${data.message}
                </div>
            `;
        } else {
            let resultsHtml = `
                <div class="alert alert-warning">
                    <strong>Processed ${data.processed || 0} of ${data.total_attempted || 0} URLs</strong>
                </div>
                <ul class="list-group mt-2">
            `;

            (data.results || []).forEach(r => {
                let badgeStyle = r.status === 'success' ? 'background-color: #198754; color: #fff;' :
                               r.status === 'skipped' ? 'background-color: #ffc107; color: #000;' : 'background-color: #dc3545; color: #fff;';
                resultsHtml += `
                    <li class="list-group-item">
                        <span class="badge" style="${badgeStyle}">${r.status}</span>
                        <small class="text-muted d-block">${r.url}</small>
                        ${r.content_preview ? `<small>${r.content_preview}</small>` : ''}
                        ${r.reason ? `<small class="text-danger">${r.reason}</small>` : ''}
                    </li>
                `;
            });

            resultsHtml += '</ul>';
            resultDiv.innerHTML = resultsHtml;
        }

        // Refresh status counts
        refreshStatus();

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
        btn.textContent = 'Process URLs';
    }
}

async function processAllUrls() {
    const processBtn = document.getElementById('processBtn');
    const processAllBtn = document.getElementById('processAllBtn');
    const stopBtn = document.getElementById('stopBtn');
    const progressContainer = document.getElementById('progressContainer');
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');
    const progressPercent = document.getElementById('progressPercent');
    const resultDiv = document.getElementById('processResult');
    const batchSizeInput = document.getElementById('batchSizeInput');
    const parallelWorkersInput = document.getElementById('parallelWorkersInput');
    const conservativeModeCheckbox = document.getElementById('conservativeModeCheckbox');
    const batchSize = parseInt(batchSizeInput.value) || 10;
    const parallelWorkers = parseInt(parallelWorkersInput.value) || 1;
    const conservativeMode = conservativeModeCheckbox.checked;

    // Disable buttons and show stop button
    processBtn.disabled = true;
    processAllBtn.disabled = true;
    stopBtn.classList.remove('d-none');
    progressContainer.classList.remove('d-none');

    processingActive = true;
    shouldStop = false;

    let totalProcessed = 0;
    let totalFailed = 0;
    let batchCount = 0;

    resultDiv.innerHTML = '<div class="alert alert-warning">Starting batch processing...</div>';

    try {
        // Get initial status
        const statusResponse = await fetch(`${API_BASE}/api/status`);

        if (!statusResponse.ok) {
            throw new Error(`Failed to get status: ${statusResponse.statusText}`);
        }

        const initialStatus = await statusResponse.json();
        const totalToProcess = initialStatus.pending ?? 0;

        if (totalToProcess === 0) {
            resultDiv.innerHTML = '<div class="alert alert-warning"><strong>No URLs to process</strong></div>';
            return;
        }

        // Process in batches until done or stopped
        while (processingActive && !shouldStop) {
            batchCount++;

            // Update progress text
            const modeText = conservativeMode ? ' (Conservative Mode)' : '';
            progressText.textContent = `Batch ${batchCount} - Processing ${batchSize} URLs with ${parallelWorkers} workers${modeText}...`;

            const response = await fetch(`${API_BASE}/api/process-urls?batch_size=${batchSize}&parallel_workers=${parallelWorkers}&conservative_mode=${conservativeMode}`, {
                method: 'POST'
            });

            if (!response.ok) {
                // Handle HTTP errors
                let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
                try {
                    const errorData = await response.json();
                    errorMessage = errorData.detail || errorMessage;
                } catch (e) {
                    // If JSON parsing fails, use the status text
                }
                throw new Error(errorMessage);
            }

            const data = await response.json();

            if (data.status === 'complete' && data.processed === 0) {
                // No more URLs to process
                break;
            }

            totalProcessed += (data.processed || 0);
            totalFailed += ((data.total_attempted || 0) - (data.processed || 0));

            // Update progress based on initial pending count
            const currentStatus = await fetch(`${API_BASE}/api/status`);

            if (!currentStatus.ok) {
                console.error('Failed to get status during batch processing');
                // Continue with last known status rather than breaking
                continue;
            }

            const status = await currentStatus.json();
            const processedInThisRun = totalToProcess - (status.pending ?? 0);
            const progress = Math.round((processedInThisRun / totalToProcess) * 100);

            progressBar.style.width = progress + '%';
            progressPercent.textContent = progress + '%';
            progressText.textContent = `Processed ${processedInThisRun} of ${totalToProcess} URLs`;

            // Show batch results
            const batchProcessed = data.processed || 0;
            const batchTotal = data.total_attempted || 0;
            const batchFailed = batchTotal - batchProcessed;

            let batchHtml = `<div class="alert alert-warning">`;
            batchHtml += `<strong>Batch ${batchCount} Complete:</strong> `;
            batchHtml += `${batchProcessed} successful, ${batchFailed} failed/skipped<br>`;
            batchHtml += `<strong>Total Progress:</strong> ${processedInThisRun} of ${totalToProcess} URLs (${progress}%)`;
            batchHtml += `</div>`;
            resultDiv.innerHTML = batchHtml;

            // Refresh status display
            await refreshStatus();

            // Check if we're done
            if (status.pending === 0) {
                break;
            }

            // Small delay between batches
            await new Promise(resolve => setTimeout(resolve, 500));
        }

        // Final update
        const finalStatus = await fetch(`${API_BASE}/api/status`);
        const final = await finalStatus.json();
        const finalProcessed = totalToProcess - final.pending;

        progressBar.style.width = '100%';
        progressPercent.textContent = '100%';
        progressText.textContent = 'Processing complete!';
        progressBar.classList.remove('progress-bar-animated');

        resultDiv.innerHTML = `
            <div class="alert alert-warning">
                <strong>Processing Complete!</strong><br>
                Total batches: ${batchCount}<br>
                Processed in this run: ${finalProcessed} of ${totalToProcess} URLs<br>
                Remaining pending: ${final.pending} URLs
            </div>
        `;

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        processingActive = false;
        processBtn.disabled = false;
        processAllBtn.disabled = false;
        stopBtn.classList.add('d-none');
    }
}

function stopProcessing() {
    shouldStop = true;
    document.getElementById('progressText').textContent = 'Stopping after current batch...';
}

async function refreshStatus() {
    try {
        const response = await fetch(`${API_BASE}/api/status`);
        const data = await response.json();

        document.getElementById('totalUrls').textContent = data.total_urls;
        document.getElementById('processedUrls').textContent = data.processed;
        document.getElementById('skippedUrls').textContent = data.skipped || 0;
        document.getElementById('failedUrls').textContent = data.failed || 0;
        document.getElementById('pendingUrls').textContent = data.pending;

        // Display recent results
        const recentDiv = document.getElementById('recentResults');
        if (data.recent_results && data.recent_results.length > 0) {
            recentDiv.innerHTML = ''; // Clear existing content

            data.recent_results.forEach((item, index) => {
                const shortContent = item.content.substring(0, 150);
                const fullContent = item.content;
                const needsExpand = fullContent.length > 150;

                // Create item container
                const itemDiv = document.createElement('div');
                itemDiv.className = 'list-group-item';
                itemDiv.id = `result-item-${index}`;

                // Format date only if available
                const dateText = item.created_at ? new Date(item.created_at).toLocaleString() : '';

                // Build the structure
                itemDiv.innerHTML = `
                    <div class="d-flex w-100 justify-content-between align-items-start">
                        <div style="flex: 1;">
                            <div class="d-flex justify-content-between align-items-start">
                                <h6 class="mb-1" style="word-break: break-all; ${dateText ? 'max-width: 85%;' : ''}">${item.url}</h6>
                                ${dateText ? `<small class="text-muted text-nowrap ms-2">${dateText}</small>` : ''}
                            </div>
                            <div class="content-preview">
                                <div class="mb-1" style="font-size: 0.875rem;" id="preview-${index}"></div>
                                <div class="full-content d-none" id="full-${index}">
                                    <div class="mb-1" style="font-size: 0.875rem;"></div>
                                </div>
                                ${needsExpand ? `
                                    <button class="btn btn-sm btn-link p-0" onclick="toggleContent(${index})">
                                        <span id="toggle-text-${index}">View Full Content</span>
                                    </button>
                                ` : ''}
                            </div>
                        </div>
                        <button class="btn btn-sm btn-danger ms-2" onclick="deleteResult('${item.url.replace(/'/g, "\\'")}', ${index})" title="Delete and reset to pending">
                            ×
                        </button>
                    </div>
                `;

                // Insert HTML content separately to avoid escaping issues
                itemDiv.querySelector(`#preview-${index}`).innerHTML = shortContent + (needsExpand ? '...' : '');
                if (needsExpand) {
                    itemDiv.querySelector(`#full-${index} > div`).innerHTML = fullContent;
                }

                recentDiv.appendChild(itemDiv);
            });
        } else {
            recentDiv.innerHTML = '<p class="text-muted">No results yet</p>';
        }

    } catch (error) {
        console.error('Status refresh error:', error);
    }
}

// Auto-refresh status every 30 seconds
setInterval(checkStatus, 30000);

// Upload URLs function
async function uploadUrls() {
    const fileInput = document.getElementById('urlFileInput');
    const uploadBtn = document.getElementById('uploadBtn');
    const resultDiv = document.getElementById('uploadResult');

    if (!fileInput.files || fileInput.files.length === 0) {
        resultDiv.innerHTML = '<div class="alert alert-warning">Please select a file</div>';
        return;
    }

    const file = fileInput.files[0];

    uploadBtn.disabled = true;
    uploadBtn.textContent = 'Uploading...';
    resultDiv.innerHTML = '<div class="alert alert-warning">Uploading URLs...</div>';

    try {
        const formData = new FormData();
        formData.append('file', file);

        const response = await fetch(`${API_BASE}/api/upload-urls`, {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (response.ok) {
            resultDiv.innerHTML = `
                <div class="alert alert-warning">
                    <strong>${data.message}</strong><br>
                    Total URLs in file: ${data.total_urls}<br>
                    New URLs added: ${data.added}<br>
                    Duplicates skipped: ${data.duplicates}
                </div>
            `;
            // Clear file input
            fileInput.value = '';
            // Refresh status
            refreshStatus();
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        uploadBtn.disabled = false;
        uploadBtn.textContent = 'Upload File';
    }
}

// Upload manual URLs function
async function uploadManualUrls() {
    const textInput = document.getElementById('manualUrlInput');
    const uploadBtn = document.getElementById('uploadManualBtn');
    const resultDiv = document.getElementById('uploadResult');

    const urlsText = textInput.value.trim();

    if (!urlsText) {
        resultDiv.innerHTML = '<div class="alert alert-warning">Please enter at least one URL</div>';
        return;
    }

    uploadBtn.disabled = true;
    uploadBtn.textContent = 'Adding...';
    resultDiv.innerHTML = '<div class="alert alert-warning">Adding URLs...</div>';

    try {
        // Convert text to file-like blob
        const blob = new Blob([urlsText], { type: 'text/plain' });
        const formData = new FormData();
        formData.append('file', blob, 'manual-urls.txt');

        const response = await fetch(`${API_BASE}/api/upload-urls`, {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (response.ok) {
            resultDiv.innerHTML = `
                <div class="alert alert-warning">
                    <strong>${data.message}</strong><br>
                    Total URLs entered: ${data.total_urls}<br>
                    New URLs added: ${data.added}<br>
                    Duplicates skipped: ${data.duplicates}
                </div>
            `;
            // Clear text input
            textInput.value = '';
            // Refresh status
            refreshStatus();
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        uploadBtn.disabled = false;
        uploadBtn.textContent = 'Add URLs';
    }
}

// Toggle content visibility
function toggleContent(index) {
    const preview = document.getElementById(`preview-${index}`);
    const full = document.getElementById(`full-${index}`);
    const toggleText = document.getElementById(`toggle-text-${index}`);

    if (full.classList.contains('d-none')) {
        preview.classList.add('d-none');
        full.classList.remove('d-none');
        toggleText.textContent = 'Contract';
    } else {
        preview.classList.remove('d-none');
        full.classList.add('d-none');
        toggleText.textContent = 'View Full Content';
    }
}

// Delete result and reset URL to pending
async function deleteResult(url, index) {
    if (!confirm(`Delete this result and reset the URL to pending?\n\n${url}`)) {
        return;
    }

    try {
        const encodedUrl = encodeURIComponent(url);
        const response = await fetch(`${API_BASE}/api/result/${encodedUrl}`, {
            method: 'DELETE'
        });

        const data = await response.json();

        if (response.ok) {
            // Remove the item from the DOM
            const item = document.getElementById(`result-item-${index}`);
            if (item) {
                item.remove();
            }

            // Refresh status to update counts
            await refreshStatus();

            // Show success message briefly
            const recentDiv = document.getElementById('recentResults');
            if (recentDiv.children.length === 0) {
                recentDiv.innerHTML = '<p class="text-muted">No results yet</p>';
            }
        } else {
            alert(`Error: ${data.detail || 'Failed to delete result'}`);
        }

    } catch (error) {
        alert(`Error: ${error.message}`);
    }
}

// Export functions
async function exportXLSX() {
    try {
        window.location.href = `${API_BASE}/api/export/xlsx`;
    } catch (error) {
        alert(`Export failed: ${error.message}`);
    }
}

async function exportJSON() {
    try {
        window.location.href = `${API_BASE}/api/export/json`;
    } catch (error) {
        alert(`Export failed: ${error.message}`);
    }
}

async function exportCombined() {
    try {
        window.location.href = `${API_BASE}/api/export/combined/xlsx`;
    } catch (error) {
        alert(`Combined export failed: ${error.message}`);
    }
}

// Validate links function
async function validateLinks() {
    const validateBtn = document.getElementById('validateBtn');
    const resultDiv = document.getElementById('validationResult');
    const batchSizeInput = document.getElementById('validationBatchSize');
    const parallelWorkersInput = document.getElementById('validationParallelWorkers');
    const batchSize = parseInt(batchSizeInput.value) || 10;
    const parallelWorkers = parseInt(parallelWorkersInput.value) || 3;

    if (batchSize < 1) {
        alert('Batch size must be at least 1');
        return;
    }

    if (parallelWorkers < 1 || parallelWorkers > 20) {
        alert('Parallel workers must be between 1 and 20');
        return;
    }

    validateBtn.disabled = true;
    validateBtn.textContent = 'Validating...';
    resultDiv.innerHTML = `<div class="alert alert-warning">Validating links in ${batchSize} content items with ${parallelWorkers} parallel workers...</div>`;

    try {
        const response = await fetch(`${API_BASE}/api/validate-links?batch_size=${batchSize}&parallel_workers=${parallelWorkers}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (data.status === 'complete' && data.validated === 0) {
            resultDiv.innerHTML = `
                <div class="alert alert-warning">
                    <strong>No content to validate</strong><br>
                    ${data.message}
                </div>
            `;
        } else {
            let summaryHtml = `
                <div class="alert alert-${data.moved_to_pending > 0 ? 'warning' : 'success'}">
                    <strong>Validation Complete</strong><br>
                    Validated: ${data.validated} items<br>
                    URLs corrected: ${data.urls_corrected || 0}<br>
                    Moved to pending (gone products): ${data.moved_to_pending}
                </div>
            `;

            // Show details for items with broken links
            if (data.results && data.results.length > 0) {
                summaryHtml += '<div class="mt-3"><strong>Results:</strong></div><ul class="list-group mt-2">';

                data.results.forEach(r => {
                    let badgeStyle = r.moved_to_pending ? 'background-color: #dc3545; color: #fff;' : 'background-color: #198754; color: #fff;';
                    let statusText = r.moved_to_pending ? 'Moved to Pending' : 'Valid';

                    summaryHtml += `
                        <li class="list-group-item">
                            <div class="d-flex justify-content-between align-items-start">
                                <div style="flex: 1;">
                                    <span class="badge" style="${badgeStyle}">${statusText}</span>
                                    <small class="text-muted d-block mt-1" style="word-break: break-all;">${r.url}</small>
                                    <small>Links: ${r.total_links} total</small>
                                    ${r.broken_links_count > 0 ? `
                                        <div class="mt-2">
                                            <strong class="text-danger">Broken Links (${r.broken_links_count}):</strong>
                                            <ul class="small mt-1">
                                                ${r.broken_links.map(bl => `
                                                    <li>
                                                        <code>${bl.url}</code>
                                                        <span class="badge bg-danger">${bl.status_code}</span>
                                                        ${bl.status_text}
                                                    </li>
                                                `).join('')}
                                            </ul>
                                        </div>
                                    ` : ''}
                                </div>
                            </div>
                        </li>
                    `;
                });

                summaryHtml += '</ul>';
            }

            resultDiv.innerHTML = summaryHtml;
        }

        // Refresh status counts
        refreshStatus();

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        validateBtn.disabled = false;
        validateBtn.textContent = 'Validate Links';
    }
}

// Reset validation history
async function resetValidationHistory() {
    if (!confirm('Reset all validation history? This will allow all URLs to be re-validated.')) {
        return;
    }

    const resetBtn = document.getElementById('resetValidationBtn');
    const resultDiv = document.getElementById('validationResult');

    resetBtn.disabled = true;
    resetBtn.textContent = 'Resetting...';
    resultDiv.innerHTML = '<div class="alert alert-warning">Resetting validation history...</div>';

    try {
        const response = await fetch(`${API_BASE}/api/validation-history/reset`, {
            method: 'DELETE'
        });

        const data = await response.json();

        if (response.ok) {
            resultDiv.innerHTML = `
                <div class="alert alert-warning">
                    <strong>${data.message}</strong><br>
                    All URLs can now be re-validated.
                </div>
            `;
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail}</div>`;
        }

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        resetBtn.disabled = false;
        resetBtn.textContent = 'Reset Validation';
    }
}

// Validate ALL links function
async function validateAllLinks() {
    const validateBtn = document.getElementById('validateBtn');
    const validateAllBtn = document.getElementById('validateAllBtn');
    const resetBtn = document.getElementById('resetValidationBtn');
    const resultDiv = document.getElementById('validationResult');
    const parallelWorkersInput = document.getElementById('validationParallelWorkers');
    const parallelWorkers = parseInt(parallelWorkersInput.value) || 3;

    if (parallelWorkers < 1 || parallelWorkers > 20) {
        alert('Parallel workers must be between 1 and 20');
        return;
    }

    // Disable buttons
    validateBtn.disabled = true;
    validateAllBtn.disabled = true;
    resetBtn.disabled = true;
    validateAllBtn.textContent = 'Validating All...';
    resultDiv.innerHTML = `<div class="alert alert-warning">Validating ALL content URLs with ${parallelWorkers} parallel workers... This may take a while.</div>`;

    try {
        const response = await fetch(`${API_BASE}/api/validate-all-links?parallel_workers=${parallelWorkers}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (data.validated === 0) {
            resultDiv.innerHTML = `
                <div class="alert alert-warning">
                    <strong>No content to validate</strong><br>
                    All URLs have already been validated.
                </div>
            `;
        } else {
            resultDiv.innerHTML = `
                <div class="alert alert-${data.moved_to_pending > 0 ? 'warning' : 'success'}">
                    <strong>Validation Complete!</strong><br>
                    Total validated: ${data.validated} items<br>
                    URLs corrected: ${data.urls_corrected || 0}<br>
                    Moved to pending (gone products): ${data.moved_to_pending}
                </div>
            `;
        }

        // Refresh status counts
        refreshStatus();

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        validateBtn.disabled = false;
        validateAllBtn.disabled = false;
        resetBtn.disabled = false;
        validateAllBtn.textContent = 'Validate All';
    }
}

// ============================================================================
// Content Publishing Functions
// ============================================================================

// Refresh publish stats on page load
window.addEventListener('DOMContentLoaded', () => {
    // Only run if on a page with publishing elements
    if (document.getElementById('publishStats')) {
        refreshPublishStats();
    }
});

async function refreshPublishStats() {
    try {
        const response = await fetch(`${API_BASE}/api/content-publish/stats`);
        const data = await response.json();

        document.getElementById('publishContentCount').textContent = data.content_top_count?.toLocaleString() || '-';
        document.getElementById('publishFaqCount').textContent = data.faq_count?.toLocaleString() || '-';
        document.getElementById('publishTotalCount').textContent = data.total_unique_urls?.toLocaleString() || '-';

    } catch (error) {
        console.error('Failed to refresh publish stats:', error);
    }
}

async function publishContent() {
    const publishBtn = document.getElementById('publishBtn');
    const resultDiv = document.getElementById('publishResult');
    const environmentSelect = document.getElementById('publishEnvironment');
    const contentTypeSelect = document.getElementById('publishContentType');

    const environment = environmentSelect.value;
    const contentType = contentTypeSelect.value;

    // Confirm for production
    if (environment === 'production') {
        if (!confirm('⚠️ WARNING: You are about to publish to PRODUCTION!\n\nAre you sure you want to continue?')) {
            return;
        }
    }

    // Disable button
    publishBtn.disabled = true;

    const contentTypeLabel = contentType === 'all' ? 'All Content' : (contentType === 'seo_only' ? 'SEO Only' : 'FAQ Only');
    resultDiv.innerHTML = `<div class="alert alert-warning">Publishing ${contentTypeLabel} to ${environment}...</div>`;

    try {
        const response = await fetch(
            `${API_BASE}/api/content-publish?environment=${environment}&content_type=${contentType}`,
            { method: 'POST' }
        );

        const data = await response.json();

        if (response.ok) {
            // Background task started - poll for status
            const taskId = data.task_id;
            resultDiv.innerHTML = `
                <div class="alert alert-warning">
                    <strong>Publishing started...</strong><br>
                    Task ID: <code>${taskId}</code><br>
                    Environment: <code>${data.environment}</code><br>
                    Content Type: <code>${data.content_type}</code><br>
                    <div class="mt-2">
                        <div class="spinner-border spinner-border-sm" role="status"></div>
                        <span id="publishStatusText">Preparing content...</span>
                    </div>
                </div>
            `;

            // Poll for status
            pollPublishStatus(taskId, resultDiv, publishBtn);
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.detail || 'Unknown error'}</div>`;
            publishBtn.disabled = false;
        }

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
        publishBtn.disabled = false;
    }
}

async function pollPublishStatus(taskId, resultDiv, publishBtn) {
    try {
        const response = await fetch(`${API_BASE}/api/content-publish/status/${taskId}`);
        const data = await response.json();

        if (data.status === 'running' || data.status === 'pending') {
            // Still running - update status text and poll again
            const statusText = document.getElementById('publishStatusText');
            if (statusText) {
                statusText.textContent = data.status === 'running' ? 'Sending content to API...' : 'Starting...';
            }
            setTimeout(() => pollPublishStatus(taskId, resultDiv, publishBtn), 2000);
        } else if (data.status === 'completed') {
            // Done - show results
            const result = data.result || {};
            const alertClass = result.success ? 'success' : 'warning';

            let html = `
                <div class="alert alert-${alertClass}">
                    <strong>Publishing Complete!</strong><br>
                    Environment: <code>${result.environment}</code><br>
                    API URL: <code>${result.api_url}</code><br>
                    Total URLs: ${result.total_urls?.toLocaleString() || 0}<br>
                    Items published: ${result.items_published?.toLocaleString() || 0}<br>
                    Payload size: ${result.payload_size_mb || 0} MB<br>
                    Status code: ${result.status_code || 'N/A'}
                </div>
            `;

            if (result.response) {
                html += `
                    <div class="alert alert-secondary mt-2">
                        <strong>API Response:</strong><br>
                        <small><code>${result.response}</code></small>
                    </div>
                `;
            }

            if (result.error) {
                html += `<div class="alert alert-danger mt-2"><strong>Error:</strong> ${result.error}</div>`;
            }

            resultDiv.innerHTML = html;
            publishBtn.disabled = false;
        } else if (data.status === 'failed') {
            // Failed
            resultDiv.innerHTML = `
                <div class="alert alert-danger">
                    <strong>Publishing Failed</strong><br>
                    Error: ${data.error || 'Unknown error'}
                </div>
            `;
            publishBtn.disabled = false;
        }
    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error checking status: ${error.message}</div>`;
        publishBtn.disabled = false;
    }
}
