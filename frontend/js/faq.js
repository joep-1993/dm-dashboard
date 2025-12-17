// FAQ Generator - Vanilla JavaScript

// Use dynamic API base - works from localhost, WSL IP, or any host
const API_BASE = window.location.origin;

let faqProcessingActive = false;
let faqShouldStop = false;

// Initialize on page load
window.addEventListener('DOMContentLoaded', () => {
    refreshFaqStatus();
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
                                <button class="btn btn-sm btn-link p-0" onclick="toggleFaqContent(${index})">
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

    if (parallelWorkers < 1 || parallelWorkers > 10) {
        alert('Parallel workers must be between 1 and 10');
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

    resultDiv.innerHTML = '<div class="alert alert-info">Starting FAQ batch processing...</div>';

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

            let batchHtml = `<div class="alert alert-info">`;
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
