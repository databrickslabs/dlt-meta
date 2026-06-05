/**
 * DLT-META Lakehouse App - Frontend Application
 * Integrates modern UI with existing Flask backend functionality
 */

// ============================================================================
// State Management
// ============================================================================

const state = {
    currentView: 'setup',
    currentCommandId: null
};

// ============================================================================
// Toast Notifications
// ============================================================================

function showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    
    const icons = {
        success: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
        error: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
        info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>'
    };

    toast.innerHTML = `
        <span class="toast-icon">${icons[type]}</span>
        <span class="toast-message">${message}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">&times;</button>
    `;

    container.appendChild(toast);
    setTimeout(() => toast.remove(), 5000);
}

// ============================================================================
// Modal Management
// ============================================================================

function showModal(title, content) {
    document.getElementById('modalTitle').textContent = title;
    document.getElementById('modalContent').innerHTML = content;
    document.getElementById('modalOverlay').classList.add('active');
}

function closeModal() {
    document.getElementById('modalOverlay').classList.remove('active');
}

function showLoadingModal() {
    document.getElementById('loadingModal').classList.add('active');
}

function hideLoadingModal() {
    document.getElementById('loadingModal').classList.remove('active');
}

// ============================================================================
// View Management
// ============================================================================

function showView(viewName) {
    // Update nav
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.toggle('active', item.dataset.view === viewName);
    });

    // Update views
    document.querySelectorAll('.view').forEach(view => {
        view.classList.toggle('active', view.id === `${viewName}View`);
    });

    // Update header
    const titles = {
        setup: { title: 'Setup', subtitle: 'Configure your DLT-META environment' },
        onboarding: { title: 'Onboarding', subtitle: 'Configure data source onboarding' },
        deployment: { title: 'Deployment', subtitle: 'Deploy DLT pipelines' },
        demos: { title: 'Demos', subtitle: 'Run pre-configured demo scenarios' },
        cli: { title: 'CLI', subtitle: 'Execute commands directly' }
    };

    const header = titles[viewName];
    if (header) {
        document.getElementById('pageTitle').textContent = header.title;
        document.getElementById('pageSubtitle').textContent = header.subtitle;
    }

    state.currentView = viewName;
}

// ============================================================================
// Form Handlers
// ============================================================================

async function handleOnboardingForm(event) {
    event.preventDefault();
    const formData = new FormData(event.target);
    
    showLoadingModal();
    
    try {
        const response = await fetch('/onboarding', {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        hideLoadingModal();
        
        if (data.modal_content) {
            const url = data.modal_content.job_url;
            const modalContent = `
                <div style="text-align: center;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" 
                         style="width: 64px; height: 64px; color: var(--accent-green); margin-bottom: 16px;">
                        <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
                        <polyline points="22 4 12 14.01 9 11.01"/>
                    </svg>
                    <h3 style="margin-bottom: 16px;">${data.modal_content.title}</h3>
                    ${data.modal_content.job_id ? `<p style="margin-bottom: 12px; color: var(--text-secondary);">Job ID: <code>${data.modal_content.job_id}</code></p>` : ''}
                    <a href="${url}" target="_blank" class="btn btn-primary" style="margin-top: 16px;">
                        Open Job in Databricks
                    </a>
                </div>
            `;
            showModal('Onboarding Complete', modalContent);
        } else {
            showModal('Response', `<pre style="color: var(--text-secondary);">${JSON.stringify(data, null, 2)}</pre>`);
        }
        
        showToast('Onboarding completed successfully', 'success');
    } catch (error) {
        hideLoadingModal();
        showToast(`Error: ${error.message}`, 'error');
    }
}

async function handleDeploymentForm(event) {
    event.preventDefault();
    const formData = new FormData(event.target);
    
    showLoadingModal();
    
    try {
        const response = await fetch('/deploy', {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        hideLoadingModal();
        
        if (data.modal_content) {
            const url = data.modal_content.job_url;
            const modalContent = `
                <div style="text-align: center;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" 
                         style="width: 64px; height: 64px; color: var(--accent-green); margin-bottom: 16px;">
                        <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
                        <polyline points="22 4 12 14.01 9 11.01"/>
                    </svg>
                    <h3 style="margin-bottom: 16px;">${data.modal_content.title}</h3>
                    ${data.modal_content.job_id ? `<p style="margin-bottom: 12px; color: var(--text-secondary);">Job ID: <code>${data.modal_content.job_id}</code></p>` : ''}
                    <a href="${url}" target="_blank" class="btn btn-primary" style="margin-top: 16px;">
                        Open Job in Databricks
                    </a>
                </div>
            `;
            showModal('Deployment Complete', modalContent);
        } else {
            showModal('Response', `<pre style="color: var(--text-secondary);">${JSON.stringify(data, null, 2)}</pre>`);
        }
        
        showToast('Deployment completed successfully', 'success');
    } catch (error) {
        hideLoadingModal();
        showToast(`Error: ${error.message}`, 'error');
    }
}

// ============================================================================
// Demo Handlers
// ============================================================================

async function runDemo(demoName) {
    const ucName = document.getElementById('demo_uc_name').value.trim();
    
    if (!ucName) {
        showToast('Please enter a Unity Catalog name', 'error');
        document.getElementById('demo_uc_name').focus();
        return;
    }
    
    showLoadingModal();
    
    try {
        const response = await fetch('/rundemo', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ demo_name: demoName, uc_name: ucName })
        });
        
        const data = await response.json();
        hideLoadingModal();
        
        if (data.modal_content) {
            const url = data.modal_content.job_url;
            const modalContent = `
                <div style="text-align: center;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" 
                         style="width: 64px; height: 64px; color: var(--accent-green); margin-bottom: 16px;">
                        <polygon points="5 3 19 12 5 21 5 3"/>
                    </svg>
                    <h3 style="margin-bottom: 16px;">${data.modal_content.title}</h3>
                    ${data.modal_content.job_id ? `<p style="margin-bottom: 12px; color: var(--text-secondary);">Job ID: <code>${data.modal_content.job_id}</code></p>` : ''}
                    <a href="${url}" target="_blank" class="btn btn-primary" style="margin-top: 16px;">
                        Open Job in Databricks
                    </a>
                </div>
            `;
            showModal('Demo Started', modalContent);
        } else {
            showModal('Response', `<pre style="color: var(--text-secondary);">${JSON.stringify(data, null, 2)}</pre>`);
        }
        
        showToast('Demo started successfully', 'success');
    } catch (error) {
        hideLoadingModal();
        showToast(`Error: ${error.message}`, 'error');
    }
}

// ============================================================================
// CLI Terminal Handlers
// ============================================================================

function appendToTerminal(text, className = '') {
    const terminal = document.getElementById('terminal');
    const line = document.createElement('div');
    line.className = `output-line ${className}`;
    
    // Convert ANSI escape codes to HTML
    const htmlContent = ansiToHtml(text);
    line.innerHTML = htmlContent;
    
    terminal.appendChild(line);
    terminal.scrollTop = terminal.scrollHeight;
}

function ansiToHtml(text) {
    // Handle bold text
    text = text.replace(/\[1m(.*?)\[0m/g, '<strong>$1</strong>');
    
    // Handle colored text
    text = text.replace(/\[36m(.*?)\[0m/g, '<span style="color:cyan">$1</span>');
    text = text.replace(/\[31m(.*?)\[0m/g, '<span style="color:red">$1</span>');
    text = text.replace(/\[32m(.*?)\[0m/g, '<span style="color:green">$1</span>');
    
    // Remove any remaining escape sequences
    text = text.replace(/\[\d+m/g, '');
    
    return text;
}

async function startCommand(command) {
    try {
        showLoadingModal();
        const response = await fetch('/start_command', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ command })
        });
        
        const data = await response.json();
        state.currentCommandId = data.command_id;
        
        // Start polling for output
        pollOutput();
        
        appendToTerminal(`$ ${command}`, 'input-line');
        document.getElementById('commandInput').value = '';
        hideLoadingModal();
    } catch (error) {
        hideLoadingModal();
        appendToTerminal(`Error: ${error.message}`, 'error-line');
    }
}

async function sendInput(input) {
    if (!state.currentCommandId) {
        appendToTerminal('No active command session', 'error-line');
        return;
    }
    
    try {
        await fetch('/send_input', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                command_id: state.currentCommandId,
                input
            })
        });
        
        document.getElementById('commandInput').value = '';
    } catch (error) {
        appendToTerminal(`Error: ${error.message}`, 'error-line');
    }
}

async function pollOutput() {
    if (!state.currentCommandId) return;
    
    try {
        const response = await fetch(`/get_output?command_id=${state.currentCommandId}`);
        const data = await response.json();
        
        if (data.status === 'success') {
            for (const item of data.output) {
                if (item.type === 'output') {
                    appendToTerminal(item.content);
                } else if (item.type === 'error') {
                    appendToTerminal(item.content, 'error-line');
                } else if (item.type === 'input') {
                    appendToTerminal(`> ${item.content}`, 'input-line');
                } else if (item.type === 'prompt') {
                    appendToTerminal(item.content, 'prompt-line');
                } else if (item.type === 'exit') {
                    if (item.content !== 0) {
                        appendToTerminal(`Command exited with code ${item.content}`, 'error-line');
                    }
                    await cleanup();
                    appendToTerminal('$ ', 'prompt');
                    return;
                }
            }
        }
        
        // Continue polling
        setTimeout(pollOutput, 500);
    } catch (error) {
        appendToTerminal(`Error polling output: ${error.message}`, 'error-line');
        setTimeout(pollOutput, 2000);
    }
}

async function cleanup() {
    if (!state.currentCommandId) return;
    
    try {
        await fetch('/cleanup', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ command_id: state.currentCommandId })
        });
        
        state.currentCommandId = null;
    } catch (error) {
        appendToTerminal(`Error during cleanup: ${error.message}`, 'error-line');
    }
}

// ============================================================================
// Event Listeners
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    // Navigation
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            showView(item.dataset.view);
        });
    });

    // Modal close
    document.getElementById('modalClose').addEventListener('click', closeModal);
    document.getElementById('modalOverlay').addEventListener('click', (e) => {
        if (e.target === e.currentTarget) closeModal();
    });

    // Escape key closes modal
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') closeModal();
    });

    // Forms
    document.getElementById('onboardingForm').addEventListener('submit', handleOnboardingForm);
    document.getElementById('deploymentForm').addEventListener('submit', handleDeploymentForm);

    // Setup button
    document.getElementById('setupBtn').addEventListener('click', () => {
        startCommand('setup');
        showView('cli');
    });

    // Demo buttons
    document.querySelectorAll('[data-demo]').forEach(btn => {
        btn.addEventListener('click', function() {
            runDemo(this.dataset.demo);
        });
    });

    // CLI
    const commandInput = document.getElementById('commandInput');
    const sendBtn = document.getElementById('sendBtn');

    sendBtn.addEventListener('click', () => {
        const input = commandInput.value.trim();
        if (!input) return;

        if (!state.currentCommandId) {
            startCommand(input);
        } else {
            sendInput(input);
        }
    });

    commandInput.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            sendBtn.click();
        }
    });

    // Quick command buttons
    document.querySelectorAll('[data-command]').forEach(btn => {
        btn.addEventListener('click', function() {
            const command = this.dataset.command;
            if (command === 'setup') {
                startCommand(command);
                showView('cli');
            } else {
                commandInput.value = command;
                commandInput.focus();
            }
        });
    });

    // Initialize terminal
    appendToTerminal('Welcome to DLT-META CLI Browser Interface', 'input-line');
    appendToTerminal('Type a command or click one of the suggestions', 'input-line');
    appendToTerminal('$ ', 'prompt');
});

