// FAQ Generator - Vanilla JavaScript

// Use dynamic API base - works from localhost, WSL IP, or any host
const API_BASE = window.location.origin;

let faqProcessingActive = false;
let faqShouldStop = false;

// Initialize on page load
window.addEventListener('DOMContentLoaded', () => {
    refreshFaqStatus();
    // Refresh publish stats if on a page with publishing elements
    if (document.getElementById('publishStats')) {
        refreshPublishStats();
    }
});

// Auto-refresh status every 30 seconds
setInterval(refreshFaqStatus, 30000);

async function refreshFaqStatus() {
    try {
        const response = await fetch(`${API_BASE}/api/faq/status`);
        const data = await response.json();

        document.getElementById('totalUrls').textContent = data.total_urls;
        document.getElementById('processedUrls').textContent = data.processed;
        document.getElementById('skippedUrls').textContent = data.skipped || 0;
        document.getElementById('failedUrls').textContent = data.failed || 0;
        document.getElementById('pendingUrls').textContent = data.pending;

        // Display recent results
        const recentDiv = document.getElementById('recentResults');
        if (data.recent_results && data.recent_results.length > 0) {
            recentDiv.innerHTML = '';

            data.recent_results.forEach((item, index) => {
                // Parse FAQ JSON to display preview
                let faqPreview = '';
                let faqCount = 0;
                try {
                    const faqs = JSON.parse(item.faq_json || '[]');
                    faqCount = faqs.length;
                    if (faqs.length > 0) {
                        // Strip HTML tags from answer to avoid broken tags in preview
                        const answerText = faqs[0].answer.replace(/<[^>]+>/g, '');
                        faqPreview = `<strong>Q:</strong> ${faqs[0].question}<br><strong>A:</strong> ${answerText.substring(0, 100)}...`;
                    }
                } catch (e) {
                    faqPreview = 'Error parsing FAQs';
                }

                const itemDiv = document.createElement('div');
                itemDiv.className = 'list-group-item';
                itemDiv.id = `faq-result-item-${index}`;

                const dateText = item.created_at ? new Date(item.created_at).toLocaleString() : '';

                // Store FAQ data in a global map for easy retrieval
                if (!window.faqDataMap) window.faqDataMap = {};
                window.faqDataMap[index] = item.faq_json || '[]';

                itemDiv.innerHTML = `
                    <div class="d-flex w-100 justify-content-between align-items-start">
                        <div style="flex: 1;">
                            <div class="d-flex justify-content-between align-items-start">
                                <div>
                                    <h6 class="mb-1" style="word-break: break-all;">${item.page_title || 'Untitled'}</h6>
                                    <small class="text-muted d-block">${item.url}</small>
                                </div>
                                ${dateText ? `<small class="text-muted text-nowrap ms-2">${dateText}</small>` : ''}
                            </div>
                            <div class="mt-2">
                                <span class="badge" style="background-color: #0dcaf0; color: #000;">${faqCount} FAQs</span>
                            </div>
                            <div class="content-preview mt-2">
                                <div class="mb-1" style="font-size: 0.875rem;" id="faq-preview-${index}">${faqPreview}</div>
                                <div class="full-content d-none" id="faq-full-${index}">
                                    <div class="mb-1" style="font-size: 0.875rem;"></div>
                                </div>
                                <button class="btn btn-sm" style="border: 1px solid #5e4a90; color: #5e4a90; background: transparent; font-size: 0.75rem; padding: 0.15rem 0.5rem;" onclick="toggleFaqContent(${index})">
                                    <span id="faq-toggle-text-${index}">View All FAQs</span>
                                </button>
                            </div>
                        </div>
                        <button class="btn btn-sm btn-danger ms-2" onclick="deleteFaqResult('${item.url.replace(/'/g, "\\'")}', ${index})" title="Delete and reset to pending">
                            x
                        </button>
                    </div>
                `;

                recentDiv.appendChild(itemDiv);
            });
        } else {
            recentDiv.innerHTML = '<p class="text-muted">No FAQ results yet</p>';
        }

    } catch (error) {
        console.error('FAQ Status refresh error:', error);
    }
}

async function processFaqUrls() {
    const btn = document.getElementById('processBtn');
    const resultDiv = document.getElementById('processResult');
    const batchSize = parseInt(document.getElementById('batchSizeInput').value) || 10;
    const parallelWorkers = parseInt(document.getElementById('parallelWorkersInput').value) || 3;
    const numFaqs = parseInt(document.getElementById('numFaqsInput').value) || 5;

    if (batchSize < 1) {
        alert('Batch size must be at least 1');
        return;
    }

    if (parallelWorkers < 1 || parallelWorkers > 20) {
        alert('Parallel workers must be between 1 and 20');
        return;
    }

    if (numFaqs < 1 || numFaqs > 10) {
        alert('Number of FAQs must be between 1 and 10');
        return;
    }

    btn.disabled = true;
    btn.textContent = 'Processing...';
    resultDiv.innerHTML = `<div class="alert alert-warning">Processing ${batchSize} URL(s) with ${parallelWorkers} parallel worker(s), generating ${numFaqs} FAQs each...</div>`;

    try {
        const response = await fetch(`${API_BASE}/api/faq/process-urls?batch_size=${batchSize}&parallel_workers=${parallelWorkers}&num_faqs=${numFaqs}`, {
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
                    <strong>Processed ${data.processed || 0} of ${data.total_attempted || 0} URLs</strong><br>
                    Skipped: ${data.skipped || 0} | Failed: ${data.failed || 0}
                </div>
                <ul class="list-group mt-2">
            `;

            (data.results || []).forEach(r => {
                let badgeStyle = r.status === 'success' ? 'background-color: #198754; color: #fff;' :
                               r.status === 'skipped' ? 'background-color: #6c757d; color: #fff;' : 'background-color: #dc3545; color: #fff;';
                resultsHtml += `
                    <li class="list-group-item">
                        <span class="badge" style="${badgeStyle}">${r.status}</span>
                        ${r.faq_count ? `<span class="badge ms-1" style="background-color: #ffc107; color: #000;">${r.faq_count} FAQs</span>` : ''}
                        <small class="text-muted d-block">${r.url}</small>
                        ${r.page_title ? `<small>${r.page_title}</small>` : ''}
                        ${r.reason ? `<small class="text-danger d-block">${r.reason}</small>` : ''}
                    </li>
                `;
            });

            resultsHtml += '</ul>';
            resultDiv.innerHTML = resultsHtml;
        }

        refreshFaqStatus();

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
        btn.textContent = 'Process URLs';
    }
}

async function processAllFaqUrls() {
    const processBtn = document.getElementById('processBtn');
    const processAllBtn = document.getElementById('processAllBtn');
    const stopBtn = document.getElementById('stopBtn');
    const progressContainer = document.getElementById('progressContainer');
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');
    const progressPercent = document.getElementById('progressPercent');
    const resultDiv = document.getElementById('processResult');
    const batchSize = parseInt(document.getElementById('batchSizeInput').value) || 10;
    const parallelWorkers = parseInt(document.getElementById('parallelWorkersInput').value) || 3;
    const numFaqs = parseInt(document.getElementById('numFaqsInput').value) || 5;

    // Disable buttons and show stop button
    processBtn.disabled = true;
    processAllBtn.disabled = true;
    stopBtn.classList.remove('d-none');
    progressContainer.classList.remove('d-none');

    faqProcessingActive = true;
    faqShouldStop = false;

    let totalProcessed = 0;
    let totalFailed = 0;
    let batchCount = 0;

    resultDiv.innerHTML = '<div class="alert alert-warning">Starting FAQ batch processing...</div>';

    try {
        // Get initial status
        const statusResponse = await fetch(`${API_BASE}/api/faq/status`);

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
        while (faqProcessingActive && !faqShouldStop) {
            batchCount++;

            progressText.textContent = `Batch ${batchCount} - Processing ${batchSize} URLs with ${parallelWorkers} workers, ${numFaqs} FAQs each...`;

            const response = await fetch(`${API_BASE}/api/faq/process-urls?batch_size=${batchSize}&parallel_workers=${parallelWorkers}&num_faqs=${numFaqs}`, {
                method: 'POST'
            });

            if (!response.ok) {
                let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
                try {
                    const errorData = await response.json();
                    errorMessage = errorData.detail || errorMessage;
                } catch (e) {}
                throw new Error(errorMessage);
            }

            const data = await response.json();

            if (data.status === 'complete' && data.processed === 0) {
                break;
            }

            totalProcessed += (data.processed || 0);
            totalFailed += ((data.total_attempted || 0) - (data.processed || 0));

            // Update progress
            const currentStatus = await fetch(`${API_BASE}/api/faq/status`);

            if (!currentStatus.ok) {
                console.error('Failed to get status during batch processing');
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

            await refreshFaqStatus();

            if (status.pending === 0) {
                break;
            }

            // Small delay between batches
            await new Promise(resolve => setTimeout(resolve, 500));
        }

        // Final update
        const finalStatus = await fetch(`${API_BASE}/api/faq/status`);
        const final = await finalStatus.json();
        const finalProcessed = totalToProcess - final.pending;

        progressBar.style.width = '100%';
        progressPercent.textContent = '100%';
        progressText.textContent = 'FAQ processing complete!';
        progressBar.classList.remove('progress-bar-animated');

        resultDiv.innerHTML = `
            <div class="alert alert-warning">
                <strong>FAQ Processing Complete!</strong><br>
                Total batches: ${batchCount}<br>
                Processed in this run: ${finalProcessed} of ${totalToProcess} URLs<br>
                Remaining pending: ${final.pending} URLs
            </div>
        `;

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        faqProcessingActive = false;
        processBtn.disabled = false;
        processAllBtn.disabled = false;
        stopBtn.classList.add('d-none');
    }
}

function stopFaqProcessing() {
    faqShouldStop = true;
    document.getElementById('progressText').textContent = 'Stopping after current batch...';
}

function toggleFaqContent(index) {
    const preview = document.getElementById(`faq-preview-${index}`);
    const full = document.getElementById(`faq-full-${index}`);
    const toggleText = document.getElementById(`faq-toggle-text-${index}`);

    if (full.classList.contains('d-none')) {
        // Show full content - get FAQ data from global map
        try {
            const faqJsonStr = window.faqDataMap ? window.faqDataMap[index] : '[]';
            const faqs = JSON.parse(faqJsonStr);
            let fullHtml = '<div class="accordion" id="faqAccordion' + index + '">';
            faqs.forEach((faq, i) => {
                fullHtml += `
                    <div class="accordion-item">
                        <h2 class="accordion-header">
                            <button class="accordion-button collapsed py-2" type="button" data-bs-toggle="collapse" data-bs-target="#faq${index}-${i}">
                                <strong>Q${i+1}:</strong>&nbsp;${faq.question}
                            </button>
                        </h2>
                        <div id="faq${index}-${i}" class="accordion-collapse collapse">
                            <div class="accordion-body py-2">
                                ${faq.answer}
                            </div>
                        </div>
                    </div>
                `;
            });
            fullHtml += '</div>';
            full.querySelector('div').innerHTML = fullHtml;
        } catch (e) {
            console.error('Error parsing FAQs:', e);
            full.querySelector('div').innerHTML = 'Error parsing FAQs';
        }

        preview.classList.add('d-none');
        full.classList.remove('d-none');
        toggleText.textContent = 'Hide FAQs';
    } else {
        preview.classList.remove('d-none');
        full.classList.add('d-none');
        toggleText.textContent = 'View All FAQs';
    }
}

async function deleteFaqResult(url, index) {
    if (!confirm(`Delete this FAQ result and reset the URL to pending?\n\n${url}`)) {
        return;
    }

    try {
        const encodedUrl = encodeURIComponent(url);
        const response = await fetch(`${API_BASE}/api/faq/result/${encodedUrl}`, {
            method: 'DELETE'
        });

        const data = await response.json();

        if (response.ok) {
            const item = document.getElementById(`faq-result-item-${index}`);
            if (item) {
                item.remove();
            }

            await refreshFaqStatus();

            const recentDiv = document.getElementById('recentResults');
            if (recentDiv.children.length === 0) {
                recentDiv.innerHTML = '<p class="text-muted">No FAQ results yet</p>';
            }
        } else {
            alert(`Error: ${data.detail || 'Failed to delete result'}`);
        }

    } catch (error) {
        alert(`Error: ${error.message}`);
    }
}

// Export functions
function exportFaqXLSX() {
    try {
        window.location.href = `${API_BASE}/api/faq/export/xlsx`;
    } catch (error) {
        alert(`Export failed: ${error.message}`);
    }
}

function exportFaqJSON() {
    try {
        window.location.href = `${API_BASE}/api/faq/export/json`;
    } catch (error) {
        alert(`Export failed: ${error.message}`);
    }
}

function exportCombined() {
    try {
        window.location.href = `${API_BASE}/api/export/combined/xlsx`;
    } catch (error) {
        alert(`Combined export failed: ${error.message}`);
    }
}

// Link validation functions
async function validateFaqLinks() {
    const btn = document.getElementById('validateBtn');
    const resultDiv = document.getElementById('validateResult');
    const batchSize = parseInt(document.getElementById('validateBatchSizeInput').value) || 500;
    const workers = parseInt(document.getElementById('validateWorkersInput').value) || 3;

    btn.disabled = true;
    btn.textContent = 'Validating...';
    resultDiv.innerHTML = `<div class="alert alert-warning">Validating ${batchSize} FAQs with ${workers} workers...</div>`;

    try {
        const response = await fetch(`${API_BASE}/api/faq/validate-links?batch_size=${batchSize}&parallel_workers=${workers}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.detail || 'Validation failed');
        }

        let alertClass = data.reset_to_pending > 0 ? 'alert-warning' : 'alert-success';
        resultDiv.innerHTML = `
            <div class="alert ${alertClass}">
                <strong>Validation Complete</strong><br>
                FAQs validated: ${data.validated}<br>
                Links checked: ${data.total_links_checked}<br>
                Valid links: ${data.valid_links}<br>
                Gone links: ${data.gone_links}<br>
                <strong>Reset to pending: ${data.reset_to_pending}</strong>
            </div>
        `;

        if (data.reset_to_pending > 0) {
            await refreshFaqStatus();
        }

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        btn.disabled = false;
        btn.textContent = 'Validate Links';
    }
}

async function validateAllFaqLinks() {
    const validateBtn = document.getElementById('validateBtn');
    const validateAllBtn = document.getElementById('validateAllBtn');
    const resetBtn = document.getElementById('resetValidationBtn');
    const resultDiv = document.getElementById('validateResult');
    const workers = parseInt(document.getElementById('validateWorkersInput').value) || 3;
    const batchSize = parseInt(document.getElementById('validateBatchSizeInput').value) || 500;

    if (!confirm('This will validate all unvalidated FAQ links. Continue?')) {
        return;
    }

    validateBtn.disabled = true;
    validateAllBtn.disabled = true;
    resetBtn.disabled = true;
    validateAllBtn.textContent = 'Validating All...';
    resultDiv.innerHTML = `<div class="alert alert-warning">Validating ALL FAQ URLs (batch size: ${batchSize}, workers: ${workers})... This may take a while.</div>`;

    try {
        const response = await fetch(`${API_BASE}/api/faq/validate-all-links?parallel_workers=${workers}&batch_size=${batchSize}`, {
            method: 'POST'
        });
        const startData = await response.json();
        const taskId = startData.task_id;

        // Poll for progress
        const poll = setInterval(async () => {
            try {
                const statusRes = await fetch(`${API_BASE}/api/faq/validate-all-links/status/${taskId}`);
                const data = await statusRes.json();

                if (data.status === 'running') {
                    resultDiv.innerHTML = `<div class="alert alert-warning">Validating... ${(data.validated || 0).toLocaleString()} FAQs processed so far. Links checked: ${(data.total_links_checked || 0).toLocaleString()}, Gone: ${data.gone_links || 0}, Reset: ${data.reset_to_pending || 0}</div>`;
                } else if (data.status === 'completed') {
                    clearInterval(poll);
                    if (data.total_links_checked === 0) {
                        resultDiv.innerHTML = `<div class="alert alert-warning"><strong>No content to validate</strong><br>All URLs have already been validated.</div>`;
                    } else {
                        resultDiv.innerHTML = `<div class="alert ${data.reset_to_pending > 0 ? 'alert-warning' : 'alert-success'}"><strong>Validation Complete!</strong><br>Total validated: ${(data.total_links_checked || 0).toLocaleString()} items<br>Gone links: ${data.gone_links || 0}<br>Moved to pending (gone products): ${data.reset_to_pending}</div>`;
                    }
                    await refreshFaqStatus();
                    validateBtn.disabled = false; validateAllBtn.disabled = false; resetBtn.disabled = false; validateAllBtn.textContent = 'Validate All';
                } else if (data.status === 'error') {
                    clearInterval(poll);
                    resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${data.error}</div>`;
                    validateBtn.disabled = false; validateAllBtn.disabled = false; resetBtn.disabled = false; validateAllBtn.textContent = 'Validate All';
                }
            } catch (e) { /* polling error, keep trying */ }
        }, 3000);

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
        validateBtn.disabled = false; validateAllBtn.disabled = false; resetBtn.disabled = false; validateAllBtn.textContent = 'Validate All';
    }
}

async function recheckSkippedFaqUrls() {
    const recheckBtn = document.getElementById('recheckSkippedBtn');
    const validateBtn = document.getElementById('validateBtn');
    const validateAllBtn = document.getElementById('validateAllBtn');
    const resetBtn = document.getElementById('resetValidationBtn');
    const resultDiv = document.getElementById('validateResult');
    const workers = parseInt(document.getElementById('validateWorkersInput').value) || 3;
    const batchSize = parseInt(document.getElementById('validateBatchSizeInput').value) || 50;

    if (workers < 1 || workers > 20) {
        alert('Parallel workers must be between 1 and 20');
        return;
    }

    if (!confirm('This will re-check all skipped FAQ URLs to see if products are now available. Continue?')) {
        return;
    }

    recheckBtn.disabled = true;
    validateBtn.disabled = true;
    validateAllBtn.disabled = true;
    resetBtn.disabled = true;
    recheckBtn.textContent = 'Rechecking...';
    resultDiv.innerHTML = `<div class="alert alert-warning">Rechecking skipped FAQ URLs (batch size: ${batchSize}, workers: ${workers})... This may take a while.</div>`;

    try {
        const response = await fetch(`${API_BASE}/api/faq/recheck-skipped-urls?parallel_workers=${workers}&batch_size=${batchSize}`, {
            method: 'POST'
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.detail || 'Recheck failed');
        }

        if (data.rechecked === 0) {
            resultDiv.innerHTML = `
                <div class="alert alert-info">
                    <strong>No skipped URLs to recheck</strong><br>
                    All skipped FAQ URLs have already been rechecked.
                </div>
            `;
        } else {
            resultDiv.innerHTML = `
                <div class="alert ${data.now_eligible > 0 ? 'alert-success' : 'alert-info'}">
                    <strong>Recheck Complete!</strong><br>
                    URLs rechecked: ${data.rechecked}<br>
                    <strong>Now eligible for FAQ generation: ${data.now_eligible}</strong>
                </div>
            `;
        }

        await refreshFaqStatus();

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        recheckBtn.disabled = false;
        validateBtn.disabled = false;
        validateAllBtn.disabled = false;
        resetBtn.disabled = false;
        recheckBtn.textContent = 'Recheck Skipped';
    }
}

async function resetFaqValidationHistory() {
    if (!confirm('Reset all validation history AND skipped URLs status? This will allow all URLs to be re-validated and rechecked.')) {
        return;
    }

    const resetBtn = document.getElementById('resetValidationBtn');
    const resultDiv = document.getElementById('validateResult');

    resetBtn.disabled = true;
    resetBtn.textContent = 'Resetting...';
    resultDiv.innerHTML = '<div class="alert alert-warning">Resetting validation history and skipped URLs...</div>';

    try {
        // Reset validation history
        const validationResponse = await fetch(`${API_BASE}/api/faq/validation-history/reset`, {
            method: 'DELETE'
        });
        const validationData = await validationResponse.json();

        // Reset skipped URLs recheck status
        const skippedResponse = await fetch(`${API_BASE}/api/faq/recheck-skipped-urls/reset`, {
            method: 'DELETE'
        });
        const skippedData = await skippedResponse.json();

        if (validationResponse.ok && skippedResponse.ok) {
            resultDiv.innerHTML = `
                <div class="alert alert-warning">
                    <strong>Reset complete:</strong><br>
                    • Validation history: ${validationData.cleared_count || 0} URLs cleared<br>
                    • Skipped URLs: ${skippedData.reset_count || 0} URLs can be rechecked
                </div>
            `;
        } else {
            const errors = [];
            if (!validationResponse.ok) errors.push(`Validation: ${validationData.detail}`);
            if (!skippedResponse.ok) errors.push(`Skipped: ${skippedData.detail}`);
            resultDiv.innerHTML = `<div class="alert alert-danger">Errors: ${errors.join(', ')}</div>`;
        }

    } catch (error) {
        resultDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    } finally {
        resetBtn.disabled = false;
        resetBtn.textContent = 'Reset Validation';
    }
}

// ============================================================================
// Content Publishing Functions
// ============================================================================

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
