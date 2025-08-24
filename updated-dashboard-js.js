// dashboard.js - Enhanced Client-side JavaScript for Production Scheduling Dashboard
// Compatible with product-specific late parts and rework tasks

let currentScenario = 'baseline';
let currentView = 'team-lead';
let selectedTeam = 'all';
let selectedShift = 'all';
let selectedProduct = 'all';
let scenarioData = {};
let allScenarios = {};
let ganttChart = null;
let ganttTasks = [];

// Initialize dashboard on page load
document.addEventListener('DOMContentLoaded', function() {
    console.log('Initializing Production Scheduling Dashboard...');
    loadAllScenarios();
    setupEventListeners();
    setupProductFilter();
});

// Load all scenarios at startup for quick switching
async function loadAllScenarios() {
    try {
        // Show loading state
        showLoading('Loading scenario data...');
        
        // Get list of scenarios
        const scenariosResponse = await fetch('/api/scenarios');
        const scenariosInfo = await scenariosResponse.json();
        
        // Load each scenario
        for (const scenario of scenariosInfo.scenarios) {
            const response = await fetch(`/api/scenario/${scenario.id}`);
            const data = await response.json();
            
            if (response.ok) {
                allScenarios[scenario.id] = data;
                console.log(`✓ Loaded ${scenario.id}: ${data.totalTasks} tasks, ${data.makespan} days makespan`);
            } else {
                console.error(`✗ Failed to load ${scenario.id}:`, data.error);
            }
        }
        
        // Set initial scenario
        scenarioData = allScenarios[currentScenario];
        
        // Update view
        hideLoading();
        updateView();
        
    } catch (error) {
        console.error('Error loading scenarios:', error);
        showError('Failed to load scenario data. Please refresh the page.');
    }
}

// Setup all event listeners
function setupEventListeners() {
    // View tabs
    document.querySelectorAll('.view-tab').forEach(tab => {
        tab.addEventListener('click', function() {
            switchView(this.dataset.view);
        });
    });

    // Scenario selection
    const scenarioSelect = document.getElementById('scenarioSelect');
    if (scenarioSelect) {
        scenarioSelect.addEventListener('change', function() {
            switchScenario(this.value);
        });
    }

    // Team selection
    const teamSelect = document.getElementById('teamSelect');
    if (teamSelect) {
        teamSelect.addEventListener('change', function() {
            selectedTeam = this.value;
            updateTeamLeadView();
        });
    }

    // Shift selection
    const shiftSelect = document.getElementById('shiftSelect');
    if (shiftSelect) {
        shiftSelect.addEventListener('change', function() {
            selectedShift = this.value;
            updateTeamLeadView();
        });
    }

    // Mechanic selection
    const mechanicSelect = document.getElementById('mechanicSelect');
    if (mechanicSelect) {
        mechanicSelect.addEventListener('change', function() {
            updateMechanicView();
        });
    }

    // Gantt filters
    const ganttProductSelect = document.getElementById('ganttProductSelect');
    if (ganttProductSelect) {
        ganttProductSelect.addEventListener('change', function() {
            filterGanttChart();
        });
    }

    const ganttTeamSelect = document.getElementById('ganttTeamSelect');
    if (ganttTeamSelect) {
        ganttTeamSelect.addEventListener('change', function() {
            filterGanttChart();
        });
    }

    const ganttSortSelect = document.getElementById('ganttSortSelect');
    if (ganttSortSelect) {
        ganttSortSelect.addEventListener('change', function() {
            sortGanttChart();
        });
    }
}

// Setup product filter
function setupProductFilter() {
    // Product filter is already in HTML, just need to populate it
    const productSelect = document.getElementById('productSelect');
    if (productSelect) {
        productSelect.addEventListener('change', function() {
            selectedProduct = this.value;
            updateTeamLeadView();
        });
    }
}

// Switch scenario with enhanced handling
function switchScenario(scenario) {
    if (allScenarios[scenario]) {
        currentScenario = scenario;
        scenarioData = allScenarios[scenario];
        
        console.log(`Switched to ${scenario}: Makespan ${scenarioData.makespan} days, Max lateness ${scenarioData.maxLateness || 0} days`);
        
        // Update product filter options
        updateProductFilter();
        
        // Show scenario-specific info
        showScenarioInfo();
        
        // Update current view
        updateView();
        
        // Re-initialize Gantt if on project view
        if (currentView === 'project') {
            initializeGanttChart();
        }
    } else {
        console.error(`Scenario ${scenario} not loaded`);
    }
}

// Update product filter dropdown
function updateProductFilter() {
    const productSelect = document.getElementById('productSelect');
    if (productSelect && scenarioData.products) {
        const currentSelection = productSelect.value;
        
        productSelect.innerHTML = '<option value="all">All Products</option>';
        scenarioData.products.forEach(product => {
            const option = document.createElement('option');
            option.value = product.name;
            option.textContent = `${product.name} (${product.totalTasks} tasks)`;
            productSelect.appendChild(option);
        });
        
        // Restore selection if possible
        if ([...productSelect.options].some(opt => opt.value === currentSelection)) {
            productSelect.value = currentSelection;
        } else {
            productSelect.value = 'all';
            selectedProduct = 'all';
        }
    }

    // CRITICAL FIX: Populate Gantt product filter
    const ganttProductSelect = document.getElementById('ganttProductSelect');
    if (ganttProductSelect) {
        const currentGanttProduct = ganttProductSelect.value;
        ganttProductSelect.innerHTML = '<option value="all">All Products</option>';
        
        // Get unique products from tasks if products array doesn't exist
        if (scenarioData.products && scenarioData.products.length > 0) {
            scenarioData.products.forEach(product => {
                const option = document.createElement('option');
                option.value = product.name;
                option.textContent = product.name;
                ganttProductSelect.appendChild(option);
            });
        } else if (scenarioData.tasks) {
            // Fallback: extract products from tasks
            const uniqueProducts = [...new Set(scenarioData.tasks.map(t => t.product))].filter(p => p);
            uniqueProducts.sort().forEach(productName => {
                const option = document.createElement('option');
                option.value = productName;
                option.textContent = productName;
                ganttProductSelect.appendChild(option);
            });
        }
        
        // Restore selection
        if (currentGanttProduct && [...ganttProductSelect.options].some(opt => opt.value === currentGanttProduct)) {
            ganttProductSelect.value = currentGanttProduct;
        }
    }

    // CRITICAL FIX: Populate Gantt team filter
    const ganttTeamSelect = document.getElementById('ganttTeamSelect');
    if (ganttTeamSelect) {
        const currentGanttTeam = ganttTeamSelect.value;
        ganttTeamSelect.innerHTML = '<option value="all">All Teams</option>';
        
        // Get unique teams from tasks or use teams array
        if (scenarioData.teams && scenarioData.teams.length > 0) {
            scenarioData.teams.forEach(team => {
                const option = document.createElement('option');
                option.value = team;
                option.textContent = team;
                ganttTeamSelect.appendChild(option);
            });
        } else if (scenarioData.tasks) {
            // Fallback: extract teams from tasks
            const uniqueTeams = [...new Set(scenarioData.tasks.map(t => t.team))].filter(t => t);
            uniqueTeams.sort().forEach(teamName => {
                const option = document.createElement('option');
                option.value = teamName;
                option.textContent = teamName;
                ganttTeamSelect.appendChild(option);
            });
        }
        
        // Restore selection
        if (currentGanttTeam && [...ganttTeamSelect.options].some(opt => opt.value === currentGanttTeam)) {
            ganttTeamSelect.value = currentGanttTeam;
        }
    }
}



// Show scenario-specific information
function showScenarioInfo() {
    let infoBanner = document.getElementById('scenarioInfo');
    if (!infoBanner) {
        const mainContent = document.querySelector('.main-content');
        infoBanner = document.createElement('div');
        infoBanner.id = 'scenarioInfo';
        mainContent.insertBefore(infoBanner, mainContent.firstChild);
    }
    
    let infoHTML = `<strong>${currentScenario.toUpperCase()}</strong>: `;
    
    if (currentScenario === 'scenario3' && scenarioData.achievedMaxLateness !== undefined) {
        if (scenarioData.achievedMaxLateness === 0) {
            infoHTML += `✓ Achieved zero lateness with ${scenarioData.totalWorkforce} workers`;
        } else if (scenarioData.achievedMaxLateness < 0) {
            infoHTML += `✓ All products ${Math.abs(scenarioData.achievedMaxLateness)} days early (${scenarioData.totalWorkforce} workers)`;
        } else {
            infoHTML += `Minimum achievable lateness: ${scenarioData.achievedMaxLateness} days (${scenarioData.totalWorkforce} workers)`;
        }
    } else if (currentScenario === 'scenario2') {
        if (scenarioData.optimalFound) {
            infoHTML += `✓ Just-in-time delivery achieved: ${scenarioData.totalWorkforce} total workers`;
        } else {
            infoHTML += `Could not achieve target delivery timing with available resources`;
        }
    } else {
        infoHTML += `Workforce: ${scenarioData.totalWorkforce}, Makespan: ${scenarioData.makespan} days`;
        if (scenarioData.maxLateness) {
            if (scenarioData.maxLateness > 0) {
                infoHTML += `, Max lateness: ${scenarioData.maxLateness} days`;
            } else {
                infoHTML += `, All products on time`;
            }
        }
    }
    
    infoBanner.innerHTML = infoHTML;
}

// Switch between views
function switchView(view) {
    document.querySelectorAll('.view-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.view-content').forEach(v => v.classList.remove('active'));
    
    document.querySelector(`[data-view="${view}"]`).classList.add('active');
    document.getElementById(`${view}-view`).classList.add('active');
    
    currentView = view;
    updateView();
    
    // Initialize Gantt when switching to project view
    if (view === 'project') {
        setTimeout(() => {
            initializeGanttChart();
        }, 100);
    }
}

// Update view based on current selection
function updateView() {
    if (!scenarioData) return;
    
    switch(currentView) {
        case 'team-lead':
            updateTeamLeadView();
            break;
        case 'management':
            updateManagementView();
            break;
        case 'mechanic':
            updateMechanicView();
            break;
        case 'project':
            initializeGanttChart();
            break;
    }
}

// Enhanced Team Lead View with product-specific filtering
async function updateTeamLeadView() {
    if (!scenarioData) return;
    
    // Update team capacity
    const teamCap = selectedTeam === 'all' ? 
        Object.values(scenarioData.teamCapacities || {}).reduce((a, b) => a + b, 0) :
        (scenarioData.teamCapacities && scenarioData.teamCapacities[selectedTeam]) || 0;
    
    document.getElementById('teamCapacity').textContent = teamCap;
    
    // Filter tasks
    let tasks = (scenarioData.tasks || []).filter(task => {
        const teamMatch = selectedTeam === 'all' || task.team === selectedTeam;
        const shiftMatch = selectedShift === 'all' || task.shift === selectedShift;
        const productMatch = selectedProduct === 'all' || task.product === selectedProduct;
        return teamMatch && shiftMatch && productMatch;
    });
    
    // Count task types
    const taskTypeCounts = {};
    tasks.forEach(task => {
        taskTypeCounts[task.type] = (taskTypeCounts[task.type] || 0) + 1;
    });
    
    // Update task counts
    const today = new Date();
    const todayTasks = tasks.filter(t => {
        const taskDate = new Date(t.startTime);
        return taskDate.toDateString() === today.toDateString();
    });
    document.getElementById('tasksToday').textContent = todayTasks.length;
    
    // Count late parts and rework
    const latePartTasks = tasks.filter(t => t.isLatePartTask).length;
    const reworkTasks = tasks.filter(t => t.isReworkTask).length;
    
    // Update utilization
    const util = selectedTeam === 'all' ? 
        scenarioData.avgUtilization || 0 :
        (scenarioData.utilization && scenarioData.utilization[selectedTeam]) || 0;
    document.getElementById('teamUtilization').textContent = Math.round(util) + '%';
    
    // Update critical tasks
    const critical = tasks.filter(t => 
        t.priority <= 10 || t.isLatePartTask || t.isReworkTask || t.isCritical
    ).length;
    document.getElementById('criticalTasks').textContent = critical;
    
    // Update task table
    const tbody = document.getElementById('taskTableBody');
    tbody.innerHTML = '';
    
    // Show top 30 tasks sorted by start time
    tasks.sort((a, b) => new Date(a.startTime) - new Date(b.startTime));
    tasks.slice(0, 100000).forEach(task => {
        const row = tbody.insertRow();
        const startTime = new Date(task.startTime);
        
        // Add special indicators
        let typeIndicator = '';
        if (task.isLatePartTask) typeIndicator = ' 📦';
        else if (task.isReworkTask) typeIndicator = ' 🔧';
        else if (task.isCritical) typeIndicator = ' ⚡';
        
        // Show dependencies if any
        let dependencyInfo = '';
        if (task.dependencies && task.dependencies.length > 0) {
            const deps = task.dependencies.slice(0, 3).map(d => 
                typeof d === 'object' ? (d.taskId || d.id) : d
            ).join(', ');
            const more = task.dependencies.length > 3 ? ` +${task.dependencies.length - 3} more` : '';
            dependencyInfo = `<span style="color: #6b7280; font-size: 11px;">Deps: ${deps}${more}</span>`;
        }
        
        row.innerHTML = `
            <td class="priority">${task.priority || '-'}</td>
            <td class="task-id">${task.taskId}${typeIndicator}</td>
            <td><span class="task-type ${getTaskTypeClass(task.type)}">${task.type}</span></td>
            <td>${task.product}<br>${dependencyInfo}</td>
            <td>${formatDateTime(startTime)}</td>
            <td>${task.duration} min</td>
            <td>${task.mechanics}</td>
            <td>
                <select class="assign-select" data-task-id="${task.taskId}">
                    <option value="">Unassigned</option>
                    <option value="mech1">John Smith</option>
                    <option value="mech2">Jane Doe</option>
                    <option value="mech3">Bob Johnson</option>
                    <option value="mech4">Alice Williams</option>
                </select>
            </td>
        `;
        
        // Highlight special rows
        if (task.isLatePartTask) {
            row.style.backgroundColor = '#fef3c7';
        } else if (task.isReworkTask) {
            row.style.backgroundColor = '#fee2e2';
        } else if (task.isCritical) {
            row.style.backgroundColor = '#dbeafe';
        }
    });
    
    // Add task type summary
    updateTaskTypeSummary(taskTypeCounts, latePartTasks, reworkTasks);
}

// Update task type summary
function updateTaskTypeSummary(taskTypeCounts, latePartCount, reworkCount) {
    let summaryDiv = document.getElementById('taskTypeSummary');
    if (!summaryDiv) {
        const statsContainer = document.querySelector('.team-stats');
        if (statsContainer) {
            summaryDiv = document.createElement('div');
            summaryDiv.id = 'taskTypeSummary';
            summaryDiv.className = 'stat-card';
            summaryDiv.style.gridColumn = 'span 2';
            statsContainer.appendChild(summaryDiv);
        }
    }
    
    if (summaryDiv) {
        let summaryHTML = '<h3>Task Type Breakdown</h3><div style="display: flex; gap: 15px; margin-top: 10px;">';
        
        for (const [type, count] of Object.entries(taskTypeCounts)) {
            const color = getTaskTypeColor(type);
            summaryHTML += `
                <div style="flex: 1;">
                    <div style="font-size: 18px; font-weight: bold; color: ${color};">${count}</div>
                    <div style="font-size: 11px; color: #6b7280;">${type}</div>
                </div>
            `;
        }
        
        summaryHTML += '</div>';
        
        if (latePartCount > 0 || reworkCount > 0) {
            summaryHTML += '<div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid #e5e7eb;">';
            summaryHTML += `<span style="margin-right: 15px;">📦 Late Parts: ${latePartCount}</span>`;
            summaryHTML += `<span>🔧 Rework: ${reworkCount}</span>`;
            summaryHTML += '</div>';
        }
        
        summaryDiv.innerHTML = summaryHTML;
    }
}

// Enhanced Management View
// Enhanced Management View with proper dates and critical task calculation
function updateManagementView() {
    if (!scenarioData) return;
    
    // Update metrics
    document.getElementById('totalWorkforce').textContent = scenarioData.totalWorkforce || 0;
    document.getElementById('makespan').textContent = scenarioData.makespan || 0;
    document.getElementById('onTimeRate').textContent = (scenarioData.onTimeRate || 0) + '%';
    document.getElementById('avgUtilization').textContent = Math.round(scenarioData.avgUtilization || 0) + '%';
    
    // Update product cards
    const productGrid = document.getElementById('productGrid');
    productGrid.innerHTML = '';
    
    (scenarioData.products || []).forEach(product => {
        // Calculate actual scheduled completion date from tasks
        const productTasks = (scenarioData.tasks || []).filter(t => t.product === product.name);
        
        let scheduledCompletion = null;
        let earliestStart = null;
        let criticalTaskCount = 0;
        
        if (productTasks.length > 0) {
            // Find the latest end time among all tasks for this product
            productTasks.forEach(task => {
                const endTime = new Date(task.endTime);
                if (!scheduledCompletion || endTime > scheduledCompletion) {
                    scheduledCompletion = endTime;
                }
                
                const startTime = new Date(task.startTime);
                if (!earliestStart || startTime < earliestStart) {
                    earliestStart = startTime;
                }
                
                // Count critical tasks (those with low slack time on critical path)
                // Critical tasks are those with less than 24 hours of slack
                if (task.slackHours !== undefined && task.slackHours < 24 && task.slackHours > -999999) {
                    criticalTaskCount++;
                }
                // Also check if task is marked as critical
                if (task.isCritical) {
                    criticalTaskCount++;
                }
            });
        }
        
        const deliveryDate = new Date(product.deliveryDate);
        const now = new Date();
        
        // Calculate status based on scheduled completion vs delivery date
        let status = 'on-time';
        let statusText = 'ON TIME';
        let daysEarlyOrLate = 0;
        
        if (scheduledCompletion) {
            const diffTime = scheduledCompletion - deliveryDate;
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            
            if (diffDays > 5) {
                status = 'late';
                statusText = 'LATE';
                daysEarlyOrLate = diffDays;
            } else if (diffDays > 0) {
                status = 'at-risk';
                statusText = 'AT RISK';
                daysEarlyOrLate = diffDays;
            } else {
                status = 'on-time';
                statusText = 'ON TIME';
                daysEarlyOrLate = Math.abs(diffDays);
            }
        }
        
        // Calculate progress based on current date
        let progress = 0;
        if (earliestStart && scheduledCompletion) {
            const totalDuration = scheduledCompletion - earliestStart;
            const elapsed = now - earliestStart;
            if (elapsed > 0) {
                progress = Math.min(100, Math.max(0, (elapsed / totalDuration) * 100));
            }
        }
        
        // Format dates for display
        const formatDate = (date) => {
            if (!date) return 'Not scheduled';
            return date.toLocaleDateString('en-US', { 
                month: 'short', 
                day: 'numeric',
                year: 'numeric'
            });
        };
        
        const card = document.createElement('div');
        card.className = 'product-card';
        card.innerHTML = `
            <div class="product-header">
                <div class="product-name">${product.name}</div>
                <div class="status-badge ${status}">${statusText}</div>
            </div>
            
            <div class="progress-bar">
                <div class="progress-fill" style="width: ${Math.round(progress)}%"></div>
            </div>
            
            <div class="product-dates">
                <div class="date-row">
                    <span class="date-label">📅 Delivery Date:</span>
                    <span class="date-value">${formatDate(deliveryDate)}</span>
                </div>
                <div class="date-row">
                    <span class="date-label">🏁 Scheduled Finish:</span>
                    <span class="date-value ${status}">${formatDate(scheduledCompletion)}</span>
                </div>
            </div>
            
            <div class="product-stats">
                <span>📅 ${Math.abs(Math.ceil((deliveryDate - now) / (1000 * 60 * 60 * 24)))} days remaining</span>
                <span>⚡ ${criticalTaskCount} critical tasks</span>
            </div>
            
            <div class="product-stats" style="margin-top: 5px; font-size: 11px;">
                <span>Tasks: ${product.totalTasks || productTasks.length}</span>
                ${product.latePartsCount > 0 ? `<span>📦 Late Parts: ${product.latePartsCount}</span>` : ''}
                ${product.reworkCount > 0 ? `<span>🔧 Rework: ${product.reworkCount}</span>` : ''}
            </div>
            
            ${status === 'late' ? `
                <div style="margin-top: 8px; padding: 5px; background: #fee2e2; border-radius: 4px; font-size: 12px; text-align: center;">
                    <strong>Late by ${daysEarlyOrLate} days</strong>
                </div>
            ` : status === 'at-risk' ? `
                <div style="margin-top: 8px; padding: 5px; background: #fef3c7; border-radius: 4px; font-size: 12px; text-align: center;">
                    <strong>At Risk - ${daysEarlyOrLate} days late</strong>
                </div>
            ` : scheduledCompletion && daysEarlyOrLate > 0 ? `
                <div style="margin-top: 8px; padding: 5px; background: #dcfce7; border-radius: 4px; font-size: 12px; text-align: center;">
                    <strong>Early by ${daysEarlyOrLate} days</strong>
                </div>
            ` : ''}
        `;
        
        // Add CSS for date display
        if (!document.getElementById('product-date-styles')) {
            const style = document.createElement('style');
            style.id = 'product-date-styles';
            style.textContent = `
                .product-dates {
                    margin: 10px 0;
                    padding: 8px;
                    background: #f9fafb;
                    border-radius: 4px;
                    font-size: 12px;
                }
                
                .date-row {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 3px 0;
                }
                
                .date-label {
                    color: #6b7280;
                    font-weight: 500;
                }
                
                .date-value {
                    color: #1f2937;
                    font-weight: 600;
                }
                
                .date-value.late {
                    color: #ef4444;
                }
                
                .date-value.at-risk {
                    color: #f59e0b;
                }
                
                .date-value.on-time {
                    color: #10b981;
                }
                
                .product-card {
                    min-height: 280px;
                }
            `;
            document.head.appendChild(style);
        }
        
        card.style.cursor = 'pointer';
        card.addEventListener('click', () => showProductDetails(product.name));
        
        productGrid.appendChild(card);
    });
    
    // Update utilization chart and late parts analysis
    updateUtilizationChart();
    updateLatePartsAnalysis();
}

// Update utilization chart
function updateUtilizationChart() {
    if (!scenarioData) return;
    
    const utilizationChart = document.getElementById('utilizationChart');
    if (!utilizationChart) return;
    
    utilizationChart.innerHTML = '';
    
    const utilization = scenarioData.utilization || {};
    
    // Sort teams by utilization
    const sortedTeams = Object.entries(utilization)
        .sort((a, b) => b[1] - a[1]);
    
    if (sortedTeams.length === 0) {
        utilizationChart.innerHTML = '<p style="text-align: center; color: #6b7280;">No utilization data available</p>';
        return;
    }
    
    // Create utilization bars
    sortedTeams.forEach(([team, util]) => {
        const utilizationItem = document.createElement('div');
        utilizationItem.className = 'utilization-item';
        
        // Determine color based on utilization level
        let fillColor = 'linear-gradient(90deg, #10b981, #10b981)'; // Green for normal
        if (util > 90) {
            fillColor = 'linear-gradient(90deg, #ef4444, #dc2626)'; // Red for high
        } else if (util > 75) {
            fillColor = 'linear-gradient(90deg, #f59e0b, #d97706)'; // Orange for medium-high
        } else if (util < 30) {
            fillColor = 'linear-gradient(90deg, #6b7280, #4b5563)'; // Gray for low
        }
        
        utilizationItem.innerHTML = `
            <div class="team-label">${team}</div>
            <div class="utilization-bar">
                <div class="utilization-fill" style="width: ${Math.min(100, util)}%; background: ${fillColor};">
                    <span class="utilization-percent">${Math.round(util)}%</span>
                </div>
            </div>
        `;
        
        utilizationChart.appendChild(utilizationItem);
    });
}

// Add this helper function to properly calculate critical tasks
function calculateCriticalTasks(tasks) {
    // Critical tasks are those on the critical path with minimal slack
    // We identify them by:
    // 1. Tasks with slack < 24 hours
    // 2. Tasks explicitly marked as critical
    // 3. Tasks that if delayed would delay the product delivery
    
    let criticalCount = 0;
    const criticalTaskIds = new Set();
    
    tasks.forEach(task => {
        // Check slack time
        if (task.slackHours !== undefined && task.slackHours !== null) {
            // Tasks with less than 24 hours slack are critical
            if (task.slackHours < 24 && task.slackHours > -999999) {
                criticalTaskIds.add(task.taskId);
            }
        }
        
        // Check if explicitly marked as critical
        if (task.isCritical) {
            criticalTaskIds.add(task.taskId);
        }
        
        // Check if it's a bottleneck task (has many dependencies)
        if (task.dependencies && task.dependencies.length > 3) {
            criticalTaskIds.add(task.taskId);
        }
        
        // Check if it's a late part or rework task (always critical)
        if (task.isLatePartTask || task.isReworkTask) {
            criticalTaskIds.add(task.taskId);
        }
    });
    
    return criticalTaskIds.size;
}

// Update late parts impact analysis
// Enhanced Late Parts Impact Analysis
// Enhanced Late Parts Impact Analysis with better error handling
async function updateLatePartsAnalysis() {
    if (!currentScenario || !scenarioData) return;
    
    // Find or create the late parts analysis section
    let analysisSection = document.querySelector('.late-parts-analysis');
    if (!analysisSection) {
        const managementView = document.getElementById('management-view');
        if (!managementView) return;
        
        analysisSection = document.createElement('div');
        analysisSection.className = 'late-parts-analysis';
        analysisSection.style.marginTop = '30px';
        
        const prioritySimulator = managementView.querySelector('.priority-simulator');
        if (prioritySimulator) {
            managementView.insertBefore(analysisSection, prioritySimulator);
        } else {
            managementView.appendChild(analysisSection);
        }
    }
    
    try {
        // Try to fetch late parts data from API
        const response = await fetch(`/api/late_parts_impact/${currentScenario}`);
        
        let data = null;
        if (response.ok) {
            data = await response.json();
        }
        
        // Build HTML display
        let html = `
            <h3 style="font-size: 18px; color: var(--dark); margin-bottom: 20px; display: flex; align-items: center; gap: 10px;">
                <span style="font-size: 24px;">📦</span>
                Late Parts Impact Analysis
            </h3>
        `;
        
        // Use data from API if available, otherwise use scenario data
        if (data && data.productImpacts && Object.keys(data.productImpacts).length > 0) {
            // Use API data
            const stats = data.overallStatistics || {};
            
            // Safely get values with defaults
            const totalLateParts = stats.totalLatePartsCount || 0;
            const makespanImpact = stats.totalMakespanImpactDays || stats.totalScheduleImpactDays || 0;
            const avgImpact = stats.averageImpactPerPart || stats.averageDelayPerPart || 0;
            const productsAffected = stats.productsWithLateParts || 0;
            const totalProducts = stats.totalProducts || scenarioData.products.length;
            
            html += `
                <div class="late-parts-summary" style="margin-bottom: 25px; padding: 20px; background: linear-gradient(135deg, #FEF3C7 0%, #FED7AA 100%); border-radius: 10px; border-left: 4px solid #F59E0B;">
                    <h4 style="margin-top: 0; color: #92400E; margin-bottom: 15px;">Overall Late Parts Impact</h4>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px;">
                        <div>
                            <div style="font-size: 24px; font-weight: bold; color: #92400E;">${totalLateParts}</div>
                            <div style="font-size: 12px; color: #78350F;">Total Late Parts</div>
                        </div>
                        <div>
                            <div style="font-size: 24px; font-weight: bold; color: #92400E;">${Number(makespanImpact).toFixed(1)}</div>
                            <div style="font-size: 12px; color: #78350F;">Impact (Days)</div>
                        </div>
                        <div>
                            <div style="font-size: 24px; font-weight: bold; color: #92400E;">${Number(avgImpact).toFixed(2)}</div>
                            <div style="font-size: 12px; color: #78350F;">Avg per Part</div>
                        </div>
                        <div>
                            <div style="font-size: 24px; font-weight: bold; color: #92400E;">${productsAffected}/${totalProducts}</div>
                            <div style="font-size: 12px; color: #78350F;">Products Affected</div>
                        </div>
                    </div>
                </div>
            `;
            
            // Product cards
            html += '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">';
            
            for (const [productName, impact] of Object.entries(data.productImpacts)) {
                const latePartCount = impact.latePartCount || 0;
                const reworkCount = impact.reworkCount || 0;
                const impactDays = Number(impact.totalMakespanImpact || impact.scheduleImpactDays || 0);
                
                const impactLevel = impactDays > 5 ? 'high' : 
                                   impactDays > 2 ? 'medium' : 
                                   impactDays > 0 ? 'low' : 'none';
                
                const cardColor = impactLevel === 'high' ? '#FEE2E2' : 
                                 impactLevel === 'medium' ? '#FEF3C7' : 
                                 impactLevel === 'low' ? '#D1FAE5' : '#FFFFFF';
                
                const borderColor = impactLevel === 'high' ? '#EF4444' : 
                                   impactLevel === 'medium' ? '#F59E0B' : 
                                   impactLevel === 'low' ? '#10B981' : '#E5E7EB';
                
                html += `
                    <div style="background: ${cardColor}; border: 2px solid ${borderColor}; border-radius: 10px; padding: 20px; position: relative;">
                        <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 15px;">
                            <div>
                                <h5 style="margin: 0; color: #1F2937; font-size: 16px;">${productName}</h5>
                                <div style="margin-top: 5px;">
                                    <span style="background: ${impact.onTime ? '#10B981' : '#EF4444'}; color: white; padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600;">
                                        ${impact.onTime ? 'ON TIME' : `LATE: ${Math.abs(impact.productLatenessDays || 0)} days`}
                                    </span>
                                </div>
                            </div>
                            <div style="text-align: right;">
                                <div style="font-size: 20px; font-weight: bold; color: ${borderColor};">${impactDays.toFixed(1)}</div>
                                <div style="font-size: 11px; color: #6B7280;">days impact</div>
                            </div>
                        </div>
                        
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; font-size: 13px;">
                            <div>
                                <span style="color: #6B7280;">Late Parts:</span>
                                <strong style="color: #1F2937; margin-left: 5px;">${latePartCount}</strong>
                            </div>
                            <div>
                                <span style="color: #6B7280;">Rework Tasks:</span>
                                <strong style="color: #1F2937; margin-left: 5px;">${reworkCount}</strong>
                            </div>
                        </div>
                    </div>
                `;
            }
            
            html += '</div>';
            
        } else {
            // Fallback: Use basic data from scenarioData
            const products = scenarioData.products || [];
            let totalLateParts = 0;
            let totalRework = 0;
            let productsWithIssues = 0;
            
            products.forEach(product => {
                const lateParts = product.latePartsCount || 0;
                const rework = product.reworkCount || 0;
                totalLateParts += lateParts;
                totalRework += rework;
                if (lateParts > 0 || rework > 0) {
                    productsWithIssues++;
                }
            });
            
            html += `
                <div class="late-parts-summary" style="margin-bottom: 20px; padding: 20px; background: #FEF3C7; border-radius: 10px; border-left: 4px solid #F59E0B;">
                    <h4 style="margin-top: 0; color: #92400E;">Overall Late Parts Impact</h4>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px;">
                        <div>
                            <div style="font-size: 24px; font-weight: bold; color: #92400E;">${totalLateParts}</div>
                            <div style="font-size: 12px; color: #78350F;">Total Late Parts</div>
                        </div>
                        <div>
                            <div style="font-size: 24px; font-weight: bold; color: #92400E;">${totalRework}</div>
                            <div style="font-size: 12px; color: #78350F;">Total Rework Tasks</div>
                        </div>
                        <div>
                            <div style="font-size: 24px; font-weight: bold; color: #92400E;">${productsWithIssues}</div>
                            <div style="font-size: 12px; color: #78350F;">Products Affected</div>
                        </div>
                        <div>
                            <div style="font-size: 24px; font-weight: bold; color: #92400E;">${products.length}</div>
                            <div style="font-size: 12px; color: #78350F;">Total Products</div>
                        </div>
                    </div>
                </div>
            `;
            
            // Product cards from scenario data
            if (products.length > 0) {
                html += '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">';
                
                products.forEach(product => {
                    const lateParts = product.latePartsCount || 0;
                    const rework = product.reworkCount || 0;
                    const hasImpact = lateParts > 0 || rework > 0;
                    
                    const cardColor = hasImpact ? '#FEF3C7' : '#FFFFFF';
                    const borderColor = hasImpact ? '#F59E0B' : '#E5E7EB';
                    
                    html += `
                        <div style="background: ${cardColor}; border: 2px solid ${borderColor}; border-radius: 10px; padding: 15px;">
                            <h5 style="margin: 0 0 10px 0; color: #1F2937;">${product.name}</h5>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; font-size: 13px;">
                                <div>
                                    <span style="color: #6B7280;">Late Parts:</span>
                                    <strong style="color: #1F2937; margin-left: 5px;">${lateParts}</strong>
                                </div>
                                <div>
                                    <span style="color: #6B7280;">Rework:</span>
                                    <strong style="color: #1F2937; margin-left: 5px;">${rework}</strong>
                                </div>
                                <div style="grid-column: span 2;">
                                    <span style="color: #6B7280;">Status:</span>
                                    <strong style="color: ${product.onTime ? '#10B981' : '#EF4444'}; margin-left: 5px;">
                                        ${product.onTime ? 'On Time' : `Late by ${product.latenessDays} days`}
                                    </strong>
                                </div>
                            </div>
                        </div>
                    `;
                });
                
                html += '</div>';
            }
        }
        
        analysisSection.innerHTML = html;
        
    } catch (error) {
        console.error('Error loading late parts analysis:', error);
        
        // Show error message
        analysisSection.innerHTML = `
            <h3 style="font-size: 18px; color: var(--dark); margin-bottom: 20px;">
                Late Parts Impact Analysis
            </h3>
            <div style="background: #FEE2E2; border: 1px solid #EF4444; padding: 20px; border-radius: 8px;">
                <strong style="color: #991B1B;">Error loading late parts analysis:</strong>
                <div style="margin-top: 10px; color: #7F1D1D;">${error.message}</div>
            </div>
        `;
    }
}

// Add to updated-dashboard-js.js

function openTeamAssignmentModal(teamName) {
    // Don't allow "all" - force selection of specific team
    if (!teamName || teamName === 'all') {
        alert('Please select a specific team (not "All Teams")');
        return;
    }
    
    // Get required capacity from CURRENT SCENARIO
    const requiredCapacity = scenarioData.teamCapacities[teamName] || 0;
    
    if (requiredCapacity === 0) {
        alert(`${teamName} has no required capacity in ${currentScenario}. This team may not be needed for this scenario.`);
        return;
    }
    
    // Create modal
    const modal = document.createElement('div');
    modal.className = 'assignment-modal';
    modal.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0,0,0,0.5);
        display: flex;
        justify-content: center;
        align-items: center;
        z-index: 10000;
    `;
    
    const modalContent = document.createElement('div');
    modalContent.style.cssText = `
        background: white;
        border-radius: 12px;
        padding: 30px;
        width: 90%;
        max-width: 1200px;
        max-height: 90vh;
        overflow-y: auto;
        position: relative;
    `;
    
    const isQualityTeam = teamName.toLowerCase().includes('quality');
    const roleLabel = isQualityTeam ? 'Quality Inspector' : 'Mechanic';
    
    // Get today's date and day of week
    const today = new Date();
    const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    const todayName = dayNames[today.getDay()];
    const isWeekend = today.getDay() === 0 || today.getDay() === 6;
    
    // Get next working day (Monday if weekend)
    const nextWorkingDay = new Date(today);
    if (today.getDay() === 6) { // Saturday
        nextWorkingDay.setDate(today.getDate() + 2); // Monday
    } else if (today.getDay() === 0) { // Sunday
        nextWorkingDay.setDate(today.getDate() + 1); // Monday
    }
    
    // Get first day of scenario schedule (usually Aug 22, 2025)
    const firstScheduleDay = new Date('2025-08-22');
    
    modalContent.innerHTML = `
        <h2 style="margin-top: 0;">Daily Assignment - ${teamName}</h2>
        <button onclick="this.closest('.assignment-modal').remove()" 
                style="position: absolute; top: 20px; right: 20px; 
                       background: none; border: none; font-size: 24px; cursor: pointer; color: #6B7280;">×</button>
        
        ${isWeekend ? `
        <div style="background: #FEF3C7; border: 1px solid #F59E0B; padding: 12px; border-radius: 8px; margin-bottom: 20px;">
            <strong style="color: #92400E;">⚠️ Today is ${todayName} (Non-Working Day)</strong><br>
            <span style="color: #78350F; font-size: 14px;">Select a working day below to view and assign tasks.</span>
        </div>
        ` : ''}
        
        <div style="background: linear-gradient(135deg, #EFF6FF 0%, #DBEAFE 100%); padding: 20px; border-radius: 8px; margin-bottom: 20px; border: 1px solid #3B82F6;">
            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px;">
                <div>
                    <label style="font-size: 12px; color: #1E40AF; display: block; font-weight: 600;">SCENARIO REQUIREMENT</label>
                    <div style="font-size: 32px; font-weight: bold; color: #1E40AF;">${requiredCapacity}</div>
                    <div style="font-size: 11px; color: #1E40AF;">${roleLabel}s needed</div>
                </div>
                <div>
                    <label style="font-size: 12px; color: #6B7280; display: block;">Planning Date</label>
                    <select id="planningDate" onchange="updateDateSelection()" style="padding: 6px; border: 1px solid #D1D5DB; border-radius: 4px; margin-top: 5px; width: 100%;">
                        <option value="today">Today (${today.toLocaleDateString()} - ${todayName})</option>
                        <option value="nextWorking" ${isWeekend ? 'selected' : ''}>Next Working Day (${nextWorkingDay.toLocaleDateString()})</option>
                        <option value="scheduleStart">Schedule Start (${firstScheduleDay.toLocaleDateString()})</option>
                        <option value="custom">Custom Date...</option>
                    </select>
                    <input type="date" id="customDate" style="display: none; margin-top: 5px; padding: 6px; border: 1px solid #D1D5DB; border-radius: 4px; width: 100%;">
                </div>
                <div>
                    <label style="font-size: 12px; color: #6B7280; display: block;">Shift</label>
                    <div style="font-size: 16px; color: #1F2937;">1st (6:00 AM - 2:30 PM)</div>
                </div>
                <div>
                    <label style="font-size: 12px; color: #6B7280; display: block;">Active Scenario</label>
                    <div style="font-size: 16px; color: #1F2937; font-weight: 600;">${currentScenario.toUpperCase()}</div>
                </div>
            </div>
        </div>
        
        <div id="dateWarning" style="display: none; background: #FEE2E2; border: 1px solid #EF4444; padding: 12px; border-radius: 8px; margin-bottom: 20px;">
            <strong style="color: #991B1B;">⚠️ Date Warning</strong>
            <div id="dateWarningMessage" style="margin-top: 5px; color: #7F1D1D;"></div>
        </div>
        
        <div style="margin-bottom: 20px;">
            <h3>Mark Attendance - Who's Here Today?</h3>
            <p style="color: #6B7280; margin-bottom: 15px;">
                ${currentScenario} requires ${requiredCapacity} ${roleLabel.toLowerCase()}s for ${teamName}. 
                Uncheck anyone who is absent.
            </p>
            
            <div style="display: flex; gap: 10px; margin-bottom: 15px;">
                <button onclick="selectAllMechanics(true)" class="btn btn-secondary" style="padding: 6px 12px; font-size: 13px;">
                    ✓ Check All Present
                </button>
                <button onclick="selectAllMechanics(false)" class="btn btn-secondary" style="padding: 6px 12px; font-size: 13px;">
                    ✗ Uncheck All
                </button>
                <span id="attendanceCount" style="margin-left: auto; padding: 8px 16px; background: #D1FAE5; border-radius: 6px; font-size: 14px; font-weight: 600; color: #065F46;">
                    ${requiredCapacity} of ${requiredCapacity} present
                </span>
            </div>
            
            <div id="attendanceList" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 10px;">
                ${generateMechanicCheckboxes(teamName, requiredCapacity)}
            </div>
        </div>
        
        <div id="shortageWarning" style="display: none; padding: 15px; background: #FEE2E2; border: 2px solid #EF4444; border-radius: 8px; margin-bottom: 20px;">
            <strong style="color: #991B1B;">⚠️ Staffing Shortage Warning</strong>
            <div id="shortageMessage" style="margin-top: 5px; color: #7F1D1D;"></div>
        </div>
        
        <button onclick="generateAssignments('${teamName}')" 
                class="btn btn-primary" 
                style="width: 100%; padding: 14px; font-size: 16px; background: #3B82F6; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 600;">
            Generate Individual Assignments
        </button>
        
        <div id="assignmentResults" style="margin-top: 30px;">
            <!-- Results will appear here -->
        </div>
    `;
    
    modal.appendChild(modalContent);
    document.body.appendChild(modal);
    
    // Add event listeners
    updateAttendanceCount();
}

// Add this new function to handle date selection changes
function updateDateSelection() {
    const planningSelect = document.getElementById('planningDate');
    const customDateInput = document.getElementById('customDate');
    const dateWarning = document.getElementById('dateWarning');
    const dateWarningMessage = document.getElementById('dateWarningMessage');
    
    if (planningSelect.value === 'custom') {
        customDateInput.style.display = 'block';
        customDateInput.valueAsDate = new Date();
        customDateInput.focus();
    } else {
        customDateInput.style.display = 'none';
    }
    
    // Check if selected date is a weekend
    let selectedDate;
    if (planningSelect.value === 'today') {
        selectedDate = new Date();
    } else if (planningSelect.value === 'nextWorking') {
        selectedDate = new Date();
        if (selectedDate.getDay() === 6) selectedDate.setDate(selectedDate.getDate() + 2);
        else if (selectedDate.getDay() === 0) selectedDate.setDate(selectedDate.getDate() + 1);
    } else if (planningSelect.value === 'scheduleStart') {
        selectedDate = new Date('2025-08-22');
    }
    
    if (selectedDate && (selectedDate.getDay() === 0 || selectedDate.getDay() === 6)) {
        dateWarning.style.display = 'block';
        dateWarningMessage.textContent = 'Selected date is a weekend. No tasks will be scheduled unless working on weekends.';
    } else {
        dateWarning.style.display = 'none';
    }
}


function generateMechanicCheckboxes(teamName, capacity) {
    let html = '';
    
    // Determine if this is a quality team or mechanic team
    const isQualityTeam = teamName.toLowerCase().includes('quality');
    const roleLabel = isQualityTeam ? 'Quality' : 'Mechanic';
    
    for (let i = 1; i <= capacity; i++) {
        const memberLabel = `${teamName} ${roleLabel} #${i}`;
        
        html += `
            <label style="display: flex; align-items: center; padding: 12px; background: white; border: 2px solid #E5E7EB; border-radius: 6px; cursor: pointer; transition: all 0.2s;">
                <input type="checkbox" 
                       value="${memberLabel}" 
                       data-member-number="${i}"
                       checked 
                       onchange="updateAttendanceCount()"
                       style="margin-right: 10px; width: 18px; height: 18px; cursor: pointer;">
                <div>
                    <div style="font-weight: 500; color: #1F2937; font-size: 14px;">${roleLabel} #${i}</div>
                    <div style="font-size: 11px; color: #6B7280;">${teamName}</div>
                </div>
            </label>
        `;
    }
    
    return html;
}

// Simplified - no need for name lookups anymore!
function getMechanicNamesForTeam(teamName, capacity) {
    // Not needed anymore - remove this function
}

function updateAttendanceCount() {
    const total = document.querySelectorAll('#attendanceList input[type="checkbox"]').length;
    const checked = document.querySelectorAll('#attendanceList input[type="checkbox"]:checked').length;
    const countElement = document.getElementById('attendanceCount');
    const warningElement = document.getElementById('shortageWarning');
    const warningMessage = document.getElementById('shortageMessage');
    
    if (countElement) {
        countElement.innerHTML = `${checked} of ${total} present`;
        
        // Change color based on attendance
        if (checked === total) {
            countElement.style.background = '#D1FAE5';
            countElement.style.color = '#065F46';
            if (warningElement) warningElement.style.display = 'none';
        } else if (checked === 0) {
            countElement.style.background = '#FEE2E2';
            countElement.style.color = '#991B1B';
            if (warningElement) {
                warningElement.style.display = 'block';
                warningMessage.innerHTML = 'No one is marked present. Please check at least one person.';
            }
        } else {
            countElement.style.background = '#FEF3C7';
            countElement.style.color = '#92400E';
            const shortage = total - checked;
            if (warningElement) {
                warningElement.style.display = 'block';
                warningMessage.innerHTML = `Short ${shortage} ${shortage === 1 ? 'person' : 'people'}. Lower priority tasks may not be assigned.`;
            }
        }
    }
}

function getMechanicNamesForTeam(teamName) {
    // In production, this would come from a database
    const names = {
        'Mechanic Team 1': ['John Smith', 'Jane Doe', 'Mike Johnson', 'Sarah Wilson', 'Tom Brown', 'Lisa Davis', 'Chris Miller'],
        'Mechanic Team 2': ['Bob Anderson', 'Alice Williams', 'David Garcia', 'Emma Martinez', 'James Taylor', 'Mary Moore', 'Robert White'],
        'Mechanic Team 3': ['Charlie Brown', 'Diana Prince', 'Frank Thomas', 'Grace Jackson', 'Henry Martin', 'Iris Lee', 'Jack Harris'],
        'Mechanic Team 4': ['Kevin Clark', 'Laura Lewis', 'Mark Walker', 'Nancy Hall', 'Oscar Allen', 'Patricia Young', 'Quinn King']
    };
    
    return names[teamName] || [];
}



async function generateAssignments(teamName) {
    // Get checked mechanics (present ones)
    const checkboxes = document.querySelectorAll('#attendanceList input[type="checkbox"]:checked');
    const presentMechanics = Array.from(checkboxes).map(cb => cb.value);
    
    if (presentMechanics.length === 0) {
        alert('Please select at least one mechanic who is present');
        return;
    }
    
    // Get selected date
    const planningSelect = document.getElementById('planningDate');
    const customDateInput = document.getElementById('customDate');
    let selectedDate;
    
    if (planningSelect.value === 'today') {
        selectedDate = new Date();
    } else if (planningSelect.value === 'nextWorking') {
        selectedDate = new Date();
        if (selectedDate.getDay() === 6) selectedDate.setDate(selectedDate.getDate() + 2);
        else if (selectedDate.getDay() === 0) selectedDate.setDate(selectedDate.getDate() + 1);
    } else if (planningSelect.value === 'scheduleStart') {
        selectedDate = new Date('2025-08-22');
    } else if (planningSelect.value === 'custom' && customDateInput.value) {
        selectedDate = new Date(customDateInput.value);
    } else {
        selectedDate = new Date(); // Default to today
    }
    
    // Get absent mechanics for display
    const allCheckboxes = document.querySelectorAll('#attendanceList input[type="checkbox"]');
    const absentMechanics = Array.from(allCheckboxes)
        .filter(cb => !cb.checked)
        .map(cb => cb.value);
    
    const resultsDiv = document.getElementById('assignmentResults');
    
    // Check if it's a weekend
    const isWeekend = selectedDate.getDay() === 0 || selectedDate.getDay() === 6;
    const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    const dayName = dayNames[selectedDate.getDay()];
    
    // Display initial status
    resultsDiv.innerHTML = `
        <div style="padding: 15px; background: #F3F4F6; border-radius: 8px; margin-bottom: 15px;">
            <strong>Assignment Parameters:</strong><br>
            Date: ${selectedDate.toLocaleDateString()} (${dayName})<br>
            Present (${presentMechanics.length}): ${presentMechanics.join(', ')}<br>
            ${absentMechanics.length > 0 ? `Absent (${absentMechanics.length}): ${absentMechanics.join(', ')}` : 'No absences'}
            ${isWeekend ? '<br><span style="color: #F59E0B; font-weight: bold;">⚠️ Weekend shift - checking for workable tasks...</span>' : ''}
        </div>
        <div class="loading">Generating optimal assignments for ${presentMechanics.length} mechanics...</div>
    `;
    
    try {
        const requestBody = {
            scenario: currentScenario,
            presentMechanics: presentMechanics,
            date: selectedDate.toISOString()
        };
        
        // For weekends, add overtime flag
        if (isWeekend) {
            requestBody.isOvertimeDay = true;
        }
        
        const response = await fetch(`/api/team/${teamName}/generate_assignments`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.error || 'Failed to generate assignments');
        }
        
        // Check if we got tasks
        if (data.teamStats.totalTasks === 0) {
            resultsDiv.innerHTML += `
                <div style="background: #FEF3C7; border: 1px solid #F59E0B; padding: 15px; border-radius: 8px; margin-top: 15px;">
                    <strong>⚠️ No tasks found for ${teamName} on ${selectedDate.toLocaleDateString()}</strong><br>
                    ${isWeekend ? 
                        'This team may have no workable tasks on weekends due to dependencies on other teams not working.' :
                        'This may be a non-working day or this team has no tasks scheduled.'
                    }<br><br>
                    <strong>Try:</strong>
                    <ul style="margin: 5px 0 0 20px;">
                        <li>Selecting "Next Working Day" or "Schedule Start" from the date dropdown</li>
                        <li>Checking if this team has tasks in ${currentScenario}</li>
                        ${isWeekend ? '<li>Coordinating with other teams to work together on weekends</li>' : ''}
                    </ul>
                </div>
            `;
        } else {
            displayAssignmentResults(data, absentMechanics);
        }
        
    } catch (error) {
        console.error('Error generating assignments:', error);
        resultsDiv.innerHTML = `
            <div style="background: #FEE2E2; border: 1px solid #EF4444; padding: 15px; border-radius: 8px;">
                <strong>❌ Error generating assignments</strong><br>
                ${error.message}<br><br>
                <details>
                    <summary style="cursor: pointer;">Technical Details</summary>
                    <pre style="margin-top: 10px; font-size: 11px;">${error.stack}</pre>
                </details>
            </div>
        `;
    }
}

// Also add this helper if you don't have it already
function selectAllMechanics(checked) {
    document.querySelectorAll('#attendanceList input[type="checkbox"]').forEach(cb => {
        cb.checked = checked;
    });
    updateAttendanceCount();
}



function displayAssignmentResults(data) {
    const resultsDiv = document.getElementById('assignmentResults');
    
    // Display warnings first
    let warningsHTML = '';
    if (data.warnings && data.warnings.length > 0) {
        warningsHTML = '<div style="margin-bottom: 20px;">';
        data.warnings.forEach(warning => {
            const bgColor = warning.level === 'critical' ? '#FEE2E2' : '#FEF3C7';
            const borderColor = warning.level === 'critical' ? '#EF4444' : '#F59E0B';
            warningsHTML += `
                <div style="padding: 10px; background: ${bgColor}; border-left: 4px solid ${borderColor}; margin-bottom: 10px; border-radius: 4px;">
                    ${warning.message}
                </div>
            `;
        });
        warningsHTML += '</div>';
    }
    
    // Display team statistics
    const stats = data.teamStats;
    let statsHTML = `
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 20px;">
            <div style="text-align: center; padding: 10px; background: #F3F4F6; border-radius: 8px;">
                <div style="font-size: 20px; font-weight: bold; color: #1F2937;">${stats.assignedTasks}/${stats.totalTasks}</div>
                <div style="font-size: 12px; color: #6B7280;">Tasks Assigned</div>
            </div>
            <div style="text-align: center; padding: 10px; background: #F3F4F6; border-radius: 8px;">
                <div style="font-size: 20px; font-weight: bold; color: #1F2937;">${stats.teamUtilization}%</div>
                <div style="font-size: 12px; color: #6B7280;">Team Utilization</div>
            </div>
            <div style="text-align: center; padding: 10px; background: ${stats.totalOvertimeMinutes > 0 ? '#FEF3C7' : '#F3F4F6'}; border-radius: 8px;">
                <div style="font-size: 20px; font-weight: bold; color: ${stats.totalOvertimeMinutes > 0 ? '#92400E' : '#1F2937'};">
                    ${Math.round(stats.totalOvertimeMinutes / 60 * 10) / 10} hrs
                </div>
                <div style="font-size: 12px; color: #6B7280;">Total Overtime</div>
            </div>
        </div>
    `;
    
    // Display individual assignments
    let assignmentsHTML = '<h3>Individual Assignments</h3>';
    assignmentsHTML += '<div style="display: grid; gap: 20px;">';
    
    for (const [mechanicName, schedule] of Object.entries(data.mechanicAssignments)) {
        const hasOvertime = schedule.overtimeMinutes > 0;
        
        assignmentsHTML += `
            <div style="border: 1px solid #E5E7EB; border-radius: 8px; padding: 15px; background: white;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h4 style="margin: 0; color: #1F2937;">${mechanicName}</h4>
                    <div style="display: flex; gap: 15px; font-size: 13px;">
                        <span>Tasks: ${schedule.tasks.length}</span>
                        <span>Utilization: ${schedule.utilizationPercent}%</span>
                        ${hasOvertime ? `<span style="color: #F59E0B; font-weight: bold;">OT: ${Math.round(schedule.overtimeMinutes)} min</span>` : ''}
                    </div>
                </div>
                
                <div style="display: grid; gap: 8px;">
                    ${schedule.tasks.map(task => `
                        <div style="display: grid; grid-template-columns: 80px 1fr auto; gap: 10px; padding: 8px; background: #F9FAFB; border-radius: 4px; align-items: center;">
                            <div style="font-weight: 600; color: #374151;">
                                ${new Date(task.startTime).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: true })}
                            </div>
                            <div>
                                <div style="font-weight: 500; color: #1F2937;">${task.displayName}</div>
                                <div style="font-size: 12px; color: #6B7280;">
                                    ${task.product} • ${task.duration} min
                                    ${task.assignedWith.length > 0 ? ` • With: ${task.assignedWith.join(', ')}` : ''}
                                </div>
                            </div>
                            <div>
                                ${task.isCritical ? '<span style="background: #3B82F6; color: white; padding: 2px 6px; border-radius: 3px; font-size: 11px;">CRITICAL</span>' : ''}
                                ${task.isLatePartTask ? '<span style="background: #F59E0B; color: white; padding: 2px 6px; border-radius: 3px; font-size: 11px;">LATE PART</span>' : ''}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    assignmentsHTML += '</div>';
    
    // Display unassigned tasks if any
    let unassignedHTML = '';
    if (data.unassignedTasks && data.unassignedTasks.length > 0) {
        unassignedHTML = `
            <h3 style="color: #EF4444; margin-top: 20px;">Unassigned Tasks (${data.unassignedTasks.length})</h3>
            <div style="border: 2px solid #EF4444; border-radius: 8px; padding: 15px; background: #FEE2E2;">
                ${data.unassignedTasks.map(task => `
                    <div style="padding: 8px; margin-bottom: 8px; background: white; border-radius: 4px;">
                        <strong>${task.taskId}</strong> - ${task.product}
                        <span style="color: #6B7280; margin-left: 10px;">${task.reason}</span>
                    </div>
                `).join('')}
            </div>
        `;
    }
    
    // Add export button
    const exportButton = `
        <button onclick="exportAssignments(${JSON.stringify(data).replace(/"/g, '&quot;')})" 
                class="btn btn-secondary" 
                style="margin-top: 20px;">
            📥 Export Assignments
        </button>
    `;
    
    resultsDiv.innerHTML = warningsHTML + statsHTML + assignmentsHTML + unassignedHTML + exportButton;
}

function exportAssignments(data) {
    // Create CSV content
    let csv = 'Mechanic,Task ID,Product,Start Time,Duration (min),Working With,Overtime (min)\n';
    
    for (const [mechanicName, schedule] of Object.entries(data.mechanicAssignments)) {
        schedule.tasks.forEach(task => {
            csv += `"${mechanicName}","${task.taskId}","${task.product}","${task.startTime}","${task.duration}","${task.assignedWith.join(', ')}","${schedule.overtimeMinutes}"\n`;
        });
    }
    
    // Download CSV
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `assignments_${data.team}_${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
}


// Function to show detailed late parts for a specific product
function showProductLatePartsDetail(productName) {
    // Fetch the detailed data for this product
    fetch(`/api/late_parts_impact/${currentScenario}`)
        .then(response => response.json())
        .then(data => {
            const productData = data.productImpacts[productName];
            if (!productData) {
                alert('No data found for ' + productName);
                return;
            }
            
            // Create modal with detailed information
            const modal = document.createElement('div');
            modal.style.cssText = `
                position: fixed;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: rgba(0,0,0,0.5);
                display: flex;
                justify-content: center;
                align-items: center;
                z-index: 10000;
            `;
            
            const modalContent = document.createElement('div');
            modalContent.style.cssText = `
                background: white;
                border-radius: 12px;
                padding: 30px;
                max-width: 800px;
                max-height: 80vh;
                overflow-y: auto;
                position: relative;
            `;
            
            let detailHTML = `
                <h3 style="margin-top: 0; color: #1F2937;">${productName} - Late Parts Detail</h3>
                <button onclick="this.closest('div[style*=fixed]').remove()" style="position: absolute; top: 20px; right: 20px; background: none; border: none; font-size: 24px; cursor: pointer; color: #6B7280;">×</button>
                
                <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0;">
                    <div style="text-align: center; padding: 10px; background: #F3F4F6; border-radius: 8px;">
                        <div style="font-size: 24px; font-weight: bold; color: #1F2937;">${productData.latePartCount}</div>
                        <div style="font-size: 12px; color: #6B7280;">Total Late Parts</div>
                    </div>
                    <div style="text-align: center; padding: 10px; background: #F3F4F6; border-radius: 8px;">
                        <div style="font-size: 24px; font-weight: bold; color: #EF4444;">${productData.totalMakespanImpact.toFixed(1)}</div>
                        <div style="font-size: 12px; color: #6B7280;">Total Impact (Days)</div>
                    </div>
                    <div style="text-align: center; padding: 10px; background: #F3F4F6; border-radius: 8px;">
                        <div style="font-size: 24px; font-weight: bold; color: #F59E0B;">${productData.criticalPathLateParts}</div>
                        <div style="font-size: 12px; color: #6B7280;">Critical Path Parts</div>
                    </div>
                    <div style="text-align: center; padding: 10px; background: #F3F4F6; border-radius: 8px;">
                        <div style="font-size: 24px; font-weight: bold; color: #3B82F6;">${productData.percentOfProductSchedule.toFixed(1)}%</div>
                        <div style="font-size: 12px; color: #6B7280;">Of Schedule</div>
                    </div>
                </div>
                
                <h4 style="margin-top: 25px; color: #1F2937;">All Late Parts (${productData.lateParts.length})</h4>
                <div style="max-height: 400px; overflow-y: auto;">
                    <table style="width: 100%; font-size: 12px;">
                        <thead style="position: sticky; top: 0; background: #F3F4F6;">
                            <tr>
                                <th style="padding: 8px; text-align: left;">Part ID</th>
                                <th style="padding: 8px; text-align: left;">Team</th>
                                <th style="padding: 8px; text-align: left;">Impact (Days)</th>
                                <th style="padding: 8px; text-align: left;">Duration (Hrs)</th>
                                <th style="padding: 8px; text-align: left;">Critical</th>
                                <th style="padding: 8px; text-align: left;">Affected Tasks</th>
                            </tr>
                        </thead>
                        <tbody>
            `;
            
            productData.lateParts.forEach((part, idx) => {
                const rowBg = idx % 2 === 0 ? '#FFFFFF' : '#F9FAFB';
                detailHTML += `
                    <tr style="background: ${rowBg};">
                        <td style="padding: 8px;">${part.displayName}</td>
                        <td style="padding: 8px;">${part.team}</td>
                        <td style="padding: 8px; color: #EF4444; font-weight: 600;">${part.makespanImpactDays.toFixed(2)}</td>
                        <td style="padding: 8px;">${part.durationHours.toFixed(1)}</td>
                        <td style="padding: 8px;">${part.isCriticalPath ? '<span style="color: #EF4444;">YES</span>' : 'No'}</td>
                        <td style="padding: 8px;">${part.affectedTaskCount}</td>
                    </tr>
                `;
            });
            
            detailHTML += `
                        </tbody>
                    </table>
                </div>
            `;
            
            modalContent.innerHTML = detailHTML;
            modal.appendChild(modalContent);
            document.body.appendChild(modal);
            
            // Close on background click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    modal.remove();
                }
            });
        })
        .catch(error => {
            alert('Error loading details: ' + error.message);
        });
}

// Function to create correlation chart
function createLatePartsCorrelationChart(correlationData) {
    const canvas = document.getElementById('latePartsCorrelationChart');
    if (!canvas || !correlationData) return;
    
    // Destroy existing chart if it exists
    if (canvas.chart) {
        canvas.chart.destroy();
    }
    
    const ctx = canvas.getContext('2d');
    
    // Prepare data
    const labels = correlationData.map(d => d.product);
    const makespanImpact = correlationData.map(d => d.makespanImpact);
    const productLateness = correlationData.map(d => d.productLateness);
    const latePartCounts = correlationData.map(d => d.latePartCount);
    
    canvas.chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Makespan Impact (days)',
                    data: makespanImpact,
                    backgroundColor: 'rgba(239, 68, 68, 0.5)',
                    borderColor: 'rgba(239, 68, 68, 1)',
                    borderWidth: 1,
                    yAxisID: 'y'
                },
                {
                    label: 'Product Lateness (days)',
                    data: productLateness,
                    type: 'line',
                    borderColor: 'rgba(59, 130, 246, 1)',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                    borderWidth: 2,
                    pointRadius: 4,
                    pointBackgroundColor: 'rgba(59, 130, 246, 1)',
                    yAxisID: 'y1'
                },
                {
                    label: 'Late Part Count',
                    data: latePartCounts,
                    type: 'scatter',
                    borderColor: 'rgba(245, 158, 11, 1)',
                    backgroundColor: 'rgba(245, 158, 11, 0.5)',
                    pointRadius: 6,
                    pointStyle: 'circle',
                    yAxisID: 'y2'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Makespan Impact (days)',
                        color: '#EF4444'
                    },
                    ticks: {
                        color: '#EF4444'
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'Product Lateness (days)',
                        color: '#3B82F6'
                    },
                    ticks: {
                        color: '#3B82F6'
                    },
                    grid: {
                        drawOnChartArea: false
                    }
                },
                y2: {
                    type: 'linear',
                    display: false
                }
            },
            plugins: {
                title: {
                    display: false
                },
                legend: {
                    position: 'top',
                    labels: {
                        usePointStyle: true,
                        padding: 15
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            if (context.dataset.label === 'Late Part Count') {
                                label += context.parsed.y + ' parts';
                            } else {
                                label += context.parsed.y.toFixed(2) + ' days';
                            }
                            return label;
                        }
                    }
                }
            }
        }
    });
}

// Make sure Chart.js is loaded
if (typeof Chart === 'undefined') {
    const script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/chart.js';
    document.head.appendChild(script);
}

// Add styles for the late parts analysis
function addLatePartsAnalysisStyles() {
    if (document.getElementById('late-parts-analysis-styles')) return;
    
    const style = document.createElement('style');
    style.id = 'late-parts-analysis-styles';
    style.textContent = `
        .late-parts-analysis-section {
            margin-top: 30px;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 8px;
        }
        
        .section-header h2 {
            color: #2c3e50;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .impact-summary-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .impact-metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        
        .metric-item {
            text-align: center;
        }
        
        .metric-value {
            font-size: 32px;
            font-weight: bold;
            color: #3b82f6;
        }
        
        .metric-label {
            font-size: 12px;
            color: #6b7280;
            margin-top: 5px;
        }
        
        .most-impacted-alert {
            background: #fef3c7;
            border: 1px solid #fbbf24;
            padding: 10px;
            border-radius: 6px;
            margin-top: 15px;
            color: #92400e;
        }
        
        .product-impact-grid {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        
        .impact-cards-container {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 20px;
            margin-top: 15px;
        }
        
        .product-impact-card {
            background: white;
            border: 2px solid #e5e7eb;
            border-radius: 8px;
            padding: 15px;
            position: relative;
            transition: all 0.3s ease;
        }
        
        .product-impact-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        
        .product-impact-card.impact-level-high {
            border-color: #ef4444;
            background: linear-gradient(135deg, #fff 0%, #fee2e2 100%);
        }
        
        .product-impact-card.impact-level-medium {
            border-color: #f59e0b;
            background: linear-gradient(135deg, #fff 0%, #fef3c7 100%);
        }
        
        .product-impact-card.impact-level-low {
            border-color: #10b981;
            background: linear-gradient(135deg, #fff 0%, #d1fae5 100%);
        }
        
        .product-impact-card.impact-level-none {
            border-color: #e5e7eb;
        }
        
        .product-impact-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        
        .product-impact-header h4 {
            margin: 0;
            color: #1f2937;
        }
        
        .flow-impact-badge {
            background: #3b82f6;
            color: white;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 14px;
            font-weight: bold;
        }
        
        .impact-details {
            font-size: 13px;
        }
        
        .detail-row {
            display: flex;
            justify-content: space-between;
            padding: 4px 0;
            border-bottom: 1px solid #f3f4f6;
        }
        
        .detail-label {
            color: #6b7280;
        }
        
        .detail-value {
            font-weight: 600;
            color: #1f2937;
        }
        
        .detail-value.critical {
            color: #ef4444;
        }
        
        .impact-bar {
            height: 8px;
            background: #e5e7eb;
            border-radius: 4px;
            margin: 15px 0 10px;
            overflow: hidden;
        }
        
        .impact-bar-fill {
            height: 100%;
            background: linear-gradient(90deg, #3b82f6, #8b5cf6);
            border-radius: 4px;
            transition: width 0.5s ease;
        }
        
        .lateness-correlation {
            font-size: 12px;
            text-align: center;
            padding: 8px;
            background: #f9fafb;
            border-radius: 4px;
            margin-top: 10px;
        }
        
        .lateness-correlation .late {
            color: #ef4444;
            font-weight: bold;
        }
        
        .lateness-correlation .on-time {
            color: #10b981;
            font-weight: bold;
        }
        
        .impact-visualization {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-top: 20px;
        }
        
        #impactCorrelationChart {
            max-height: 400px;
        }
    `;
    document.head.appendChild(style);
}

// Create correlation chart
function createImpactCorrelationChart(correlationData) {
    const canvas = document.getElementById('impactCorrelationChart');
    if (!canvas || !correlationData) return;
    
    const ctx = canvas.getContext('2d');
    
    // Prepare data
    const labels = correlationData.map(d => d.product);
    const flowImpact = correlationData.map(d => d.flowImpact);
    const lateness = correlationData.map(d => d.lateness);
    
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Flow Days Impact',
                    data: flowImpact,
                    backgroundColor: 'rgba(59, 130, 246, 0.5)',
                    borderColor: 'rgba(59, 130, 246, 1)',
                    borderWidth: 1,
                    yAxisID: 'y'
                },
                {
                    label: 'Product Lateness (days)',
                    data: lateness,
                    type: 'line',
                    borderColor: 'rgba(239, 68, 68, 1)',
                    backgroundColor: 'rgba(239, 68, 68, 0.1)',
                    borderWidth: 2,
                    pointRadius: 4,
                    pointBackgroundColor: 'rgba(239, 68, 68, 1)',
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Flow Days Impact'
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'Product Lateness (days)'
                    },
                    grid: {
                        drawOnChartArea: false
                    }
                }
            },
            plugins: {
                title: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            label += context.parsed.y.toFixed(2) + ' days';
                            return label;
                        }
                    }
                }
            }
        }
    });
}

// Call this when switching views or scenarios
if (currentView === 'management') {
    updateLatePartsAnalysis();
}

// Enhanced Individual Mechanic View
async function updateMechanicView() {
    if (!scenarioData) return;
    
    const mechanicId = document.getElementById('mechanicSelect').value;
    
    // Simulate mechanic data (replace with actual API call)
    const mechanicTasks = (scenarioData.tasks || [])
        .filter(t => t.assignedTo === mechanicId || Math.random() > 0.8)
        .slice(0, 10);
    
    // Update stats
    document.getElementById('currentShift').textContent = '1st Shift';
    document.getElementById('tasksAssigned').textContent = mechanicTasks.length;
    
    if (mechanicTasks.length > 0) {
        const lastTask = mechanicTasks[mechanicTasks.length - 1];
        const endTime = new Date(lastTask.endTime || lastTask.startTime);
        document.getElementById('estCompletion').textContent = formatTime(endTime);
    } else {
        document.getElementById('estCompletion').textContent = 'No tasks';
    }
    
    // Update timeline
    const timeline = document.getElementById('mechanicTimeline');
    timeline.innerHTML = '';
    
    mechanicTasks.forEach(task => {
        const startTime = new Date(task.startTime);
        const item = document.createElement('div');
        item.className = 'timeline-item';
        
        if (task.isLatePartTask) {
            item.style.borderLeftColor = '#f59e0b';
        } else if (task.isReworkTask) {
            item.style.borderLeftColor = '#ef4444';
        } else if (task.isCritical) {
            item.style.borderLeftColor = '#3b82f6';
        }
        
        let dependencyWarning = '';
        if (task.dependencies && task.dependencies.length > 0) {
            const deps = task.dependencies.slice(0, 2).map(d => 
                typeof d === 'object' ? (d.taskId || d.id) : d
            ).join(', ');
            dependencyWarning = `
                <div class="dependency-warning">
                    ⚠️ Waiting on: ${deps}
                </div>
            `;
        }
        
        item.innerHTML = `
            <div class="timeline-time">${formatTime(startTime)}</div>
            <div class="timeline-content">
                <div class="timeline-task">
                    ${task.taskId} - ${task.type}
                    ${task.isLatePartTask ? ' 📦' : ''}
                    ${task.isReworkTask ? ' 🔧' : ''}
                    ${task.isCritical ? ' ⚡' : ''}
                </div>
                <div class="timeline-details">
                    <span>📦 ${task.product}</span>
                    <span>⏱️ ${task.duration} minutes</span>
                    <span>👥 ${task.mechanics} mechanic(s)</span>
                </div>
                ${dependencyWarning}
            </div>
        `;
        timeline.appendChild(item);
    });
}

// Initialize Gantt Chart with enhanced features
function initializeGanttChart() {
    if (!scenarioData || !scenarioData.tasks) return;
    
    // Populate filters before initializing chart
    updateProductFilter();
    
    const ganttContainer = document.getElementById('ganttChart');
    if (!ganttContainer) return;
    
    // Clear existing chart
    ganttContainer.innerHTML = '';
    
    // Add legend
    const legend = document.createElement('div');
    legend.className = 'gantt-legend';
    legend.innerHTML = `
        <div class="gantt-legend-item">
            <div class="gantt-legend-box" style="background: #DC2626;"></div>
            <span>Product A</span>
        </div>
        <div class="gantt-legend-item">
            <div class="gantt-legend-box" style="background: #FCD34D;"></div>
            <span>Product B</span>
        </div>
        <div class="gantt-legend-item">
            <div class="gantt-legend-box" style="background: #10B981;"></div>
            <span>Product C</span>
        </div>
        <div class="gantt-legend-item">
            <div class="gantt-legend-box" style="background: #3B82F6;"></div>
            <span>Product D</span>
        </div>
        <div class="gantt-legend-item">
            <div class="gantt-legend-box" style="background: #8B5CF6;"></div>
            <span>Product E</span>
        </div>
        <div class="gantt-legend-item">
            <div class="gantt-legend-box critical"></div>
            <span>Critical Path</span>
        </div>
        <div class="gantt-legend-item">
            <div class="gantt-legend-box late-part"></div>
            <span>Late Part</span>
        </div>
        <div class="gantt-legend-item">
            <div class="gantt-legend-box rework"></div>
            <span>Rework</span>
        </div>
    `;
    ganttContainer.appendChild(legend);
    
    // Create chart container
    const chartDiv = document.createElement('div');
    chartDiv.id = 'ganttChartSvg';
    ganttContainer.appendChild(chartDiv);
    
    // Prepare tasks for Gantt
    ganttTasks = prepareGanttTasks(scenarioData.tasks);
    
    if (ganttTasks.length === 0) {
        chartDiv.innerHTML = '<p style="text-align: center; padding: 40px;">No tasks to display</p>';
        return;
    }
    
    // Initialize Gantt chart with enhanced configuration
    try {
        ganttChart = new Gantt("#ganttChartSvg", ganttTasks, {
            view_mode: 'Day',
            date_format: 'YYYY-MM-DD',
            custom_popup_html: function(task) {
                return createEnhancedTaskPopup(task);
            },
            on_click: function(task) {
                // Force popup to show on click
                console.log('Task clicked:', task.id);
                return createEnhancedTaskPopup(task);
            },
            on_date_change: function(task, start, end) {
                console.log(`Task ${task.id} date changed`);
                return false; // Prevent date changes if needed
            },
            on_progress_change: function(task, progress) {
                console.log(`Task ${task.id} progress changed to ${progress}`);
                return false; // Prevent progress changes if needed
            },
            on_view_change: function(mode) {
                console.log(`View changed to ${mode}`);
                setTimeout(() => {
                    highlightNonWorkDays();
                    fixTaskClickability();
                }, 100);
            },
            bar_height: 20,     // Ensure bars are tall enough to click
            padding: 18,        // Add padding between bars
            view_modes: ['Quarter Day', 'Half Day', 'Day', 'Week', 'Month'],
            popup_trigger: 'click',  // Ensure popup triggers on click
            language: 'en'
        });
        
        // Apply custom styling and highlight non-work days
        setTimeout(() => {
            applyGanttStyling();
            highlightNonWorkDays();
            fixTaskClickability();
        }, 100);
        
    } catch (error) {
        console.error('Error initializing Gantt chart:', error);
        chartDiv.innerHTML = '<p style="text-align: center; padding: 40px; color: red;">Error loading Gantt chart</p>';
    }
}

// Fix task clickability issues
function fixTaskClickability() {
    // Ensure all task bars are clickable
    const taskBars = document.querySelectorAll('#ganttChartSvg .bar-wrapper');
    
    taskBars.forEach(bar => {
        // Remove any blocking elements
        bar.style.pointerEvents = 'auto';
        bar.style.cursor = 'pointer';
        
        // Find the rect element inside
        const rect = bar.querySelector('.bar');
        if (rect) {
            rect.style.pointerEvents = 'auto';
            rect.style.cursor = 'pointer';
        }
        
        // Add backup click handler if main one fails
        if (!bar.hasAttribute('data-click-fixed')) {
            bar.setAttribute('data-click-fixed', 'true');
            
            bar.addEventListener('click', function(e) {
                e.stopPropagation();
                
                // Find the task ID from the bar
                const taskGroup = bar.closest('g[data-id]');
                if (taskGroup) {
                    const taskId = taskGroup.getAttribute('data-id');
                    const task = ganttTasks.find(t => t.id === taskId);
                    
                    if (task) {
                        console.log('Backup click handler for task:', taskId);
                        showTaskPopupManually(task);
                    }
                }
            });
        }
    });
    
    // Fix z-index issues
    const style = document.createElement('style');
    style.textContent = `
        #ganttChartSvg .bar-wrapper {
            z-index: 10 !important;
            pointer-events: auto !important;
            cursor: pointer !important;
        }
        
        #ganttChartSvg .bar {
            pointer-events: auto !important;
            cursor: pointer !important;
        }
        
        #ganttChartSvg .bar-wrapper:hover .bar {
            opacity: 0.8 !important;
            stroke: #333 !important;
            stroke-width: 2px !important;
        }
        
        /* Ensure popup is above everything */
        .popup-wrapper {
            z-index: 1000 !important;
            pointer-events: auto !important;
        }
        
        /* Fix for small bars */
        #ganttChartSvg .bar-wrapper .bar {
            min-height: 18px !important;
        }
        
        /* Ensure handle bars don't block clicks */
        #ganttChartSvg .handle-group {
            pointer-events: none !important;
        }
        
        #ganttChartSvg .handle-group .handle {
            pointer-events: auto !important;
        }
    `;
    
    // Remove old style if exists
    const oldStyle = document.getElementById('gantt-clickability-fix');
    if (oldStyle) {
        oldStyle.remove();
    }
    
    style.id = 'gantt-clickability-fix';
    document.head.appendChild(style);
}

// Manual popup display function as fallback
function showTaskPopupManually(task) {
    // Remove any existing popup
    const existingPopup = document.querySelector('.popup-wrapper');
    if (existingPopup) {
        existingPopup.remove();
    }
    
    // Create popup wrapper
    const popupWrapper = document.createElement('div');
    popupWrapper.className = 'popup-wrapper';
    popupWrapper.style.cssText = `
        position: fixed;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        z-index: 10000;
        background: white;
        border-radius: 8px;
        box-shadow: 0 10px 40px rgba(0,0,0,0.2);
        max-width: 450px;
        max-height: 80vh;
        overflow-y: auto;
    `;
    
    // Add popup content
    popupWrapper.innerHTML = createEnhancedTaskPopup(task);
    
    // Add close button
    const closeBtn = document.createElement('button');
    closeBtn.innerHTML = '✕';
    closeBtn.style.cssText = `
        position: absolute;
        top: 10px;
        right: 10px;
        background: none;
        border: none;
        font-size: 20px;
        cursor: pointer;
        color: #6b7280;
        width: 30px;
        height: 30px;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 4px;
        transition: all 0.2s;
    `;
    closeBtn.onmouseover = () => {
        closeBtn.style.background = '#f3f4f6';
        closeBtn.style.color = '#1f2937';
    };
    closeBtn.onmouseout = () => {
        closeBtn.style.background = 'none';
        closeBtn.style.color = '#6b7280';
    };
    closeBtn.onclick = () => popupWrapper.remove();
    
    popupWrapper.firstElementChild.appendChild(closeBtn);
    
    // Add backdrop
    const backdrop = document.createElement('div');
    backdrop.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0,0,0,0.3);
        z-index: 9999;
    `;
    backdrop.onclick = () => {
        popupWrapper.remove();
        backdrop.remove();
    };
    
    // Add to document
    document.body.appendChild(backdrop);
    document.body.appendChild(popupWrapper);
}



// Prepare tasks with full dependency information (SINGLE CLEAN VERSION)
// Enhanced prepareGanttTasks with better dependency handling and debugging
// Enhanced prepareGanttTasks with better debugging and property detection
function prepareGanttTasks(tasks) {
    if (!tasks || tasks.length === 0) return [];
    
    console.log('Preparing Gantt tasks, total tasks:', tasks.length);
    
    // First, create a complete task map
    const taskMap = {};
    tasks.forEach(task => {
        taskMap[task.taskId] = task;
    });
    
    // Build complete dependency and successor maps
    const dependencyMap = {};
    const successorMap = {};
    
    // Initialize maps
    tasks.forEach(task => {
        dependencyMap[task.taskId] = [];
        successorMap[task.taskId] = [];
    });
    
    // Debug: Deep inspect the dependency structure
    let sampleTask = tasks.find(t => t.dependencies && t.dependencies.length > 0);
    if (sampleTask) {
        console.log('Sample task with dependencies:', sampleTask.taskId);
        console.log('Dependencies structure:', sampleTask.dependencies);
        console.log('First dependency object:', sampleTask.dependencies[0]);
        
        // CRITICAL: Log all properties of the first dependency
        if (sampleTask.dependencies[0] && typeof sampleTask.dependencies[0] === 'object') {
            console.log('Dependency object properties:', Object.keys(sampleTask.dependencies[0]));
            console.log('Dependency object values:', sampleTask.dependencies[0]);
            
            // Try to find the task ID property
            const dep = sampleTask.dependencies[0];
            console.log('Possible taskId values:');
            console.log('  - taskId:', dep.taskId);
            console.log('  - task_id:', dep.task_id);
            console.log('  - id:', dep.id);
            console.log('  - task:', dep.task);
            console.log('  - dependencyTaskId:', dep.dependencyTaskId);
            console.log('  - predecessor:', dep.predecessor);
            console.log('  - from:', dep.from);
            console.log('  - source:', dep.source);
            
            // Log ALL properties and values
            for (let key in dep) {
                console.log(`  - ${key}:`, dep[key]);
            }
        }
    }
    
    // Process all dependencies with comprehensive format support
    tasks.forEach(task => {
        if (task.dependencies && task.dependencies.length > 0) {
            task.dependencies.forEach(dep => {
                let depTaskId = null;
                let depType = 'Finish-Start';
                
                // Try every possible way to get the task ID
                if (typeof dep === 'string') {
                    depTaskId = dep;
                } else if (typeof dep === 'object' && dep !== null) {
                    // Try all possible property names for task ID
                    depTaskId = dep.taskId || 
                               dep.task_id || 
                               dep.id || 
                               dep.task || 
                               dep.dependencyTaskId || 
                               dep.predecessor || 
                               dep.from || 
                               dep.source ||
                               dep.dependent_task_id ||
                               dep.pred_task_id ||
                               dep.parent ||
                               dep.parent_task ||
                               (dep[0] && typeof dep[0] === 'string' ? dep[0] : null);
                    
                    // If still no ID, try to get the first string property
                    if (!depTaskId) {
                        for (let key in dep) {
                            if (typeof dep[key] === 'string' && dep[key].match(/^[A-Z]_\d+$/)) {
                                depTaskId = dep[key];
                                console.log(`Found task ID in property '${key}': ${depTaskId}`);
                                break;
                            }
                        }
                    }
                    
                    // Try to get dependency type
                    depType = dep.type || 
                             dep.dependency_type || 
                             dep.dependencyType || 
                             dep.relationship || 
                             dep.link_type ||
                             'Finish-Start';
                }
                
                if (depTaskId) {
                    // Verify the dependency task exists
                    const depTask = taskMap[depTaskId];
                    if (!depTask) {
                        console.warn(`Dependency ${depTaskId} not found for task ${task.taskId}`);
                        return;
                    }
                    
                    // Add to dependency map (predecessors)
                    dependencyMap[task.taskId].push({
                        taskId: depTaskId,
                        type: depType,
                        team: depTask.team || 'Unknown Team'
                    });
                    
                    // Add to successor map
                    if (!successorMap[depTaskId]) {
                        successorMap[depTaskId] = [];
                    }
                    successorMap[depTaskId].push({
                        taskId: task.taskId,
                        type: depType,
                        team: task.team || 'Unknown Team'
                    });
                    
                    console.log(`Mapped dependency: ${depTaskId} -> ${task.taskId}`);
                } else {
                    console.warn(`Could not extract task ID from dependency for task ${task.taskId}:`, dep);
                }
            });
        }
    });
    
    // Debug: Log final statistics
    const tasksWithDeps = Object.keys(dependencyMap).filter(id => dependencyMap[id].length > 0);
    const tasksWithSuccs = Object.keys(successorMap).filter(id => successorMap[id].length > 0);
    console.log(`✅ Tasks with predecessors: ${tasksWithDeps.length}`);
    console.log(`✅ Tasks with successors: ${tasksWithSuccs.length}`);
    
    if (tasksWithDeps.length > 0) {
        const sampleId = tasksWithDeps[0];
        console.log(`Sample - Task ${sampleId} has ${dependencyMap[sampleId].length} predecessors:`, dependencyMap[sampleId]);
    }
    
    if (tasksWithSuccs.length > 0) {
        const sampleId = tasksWithSuccs[0];
        console.log(`Sample - Task ${sampleId} has ${successorMap[sampleId].length} successors:`, successorMap[sampleId]);
    }
    
    // Create Gantt tasks with complete information
    return tasks.map(task => {
        const startDate = new Date(task.startTime);
        let endDate = new Date(task.endTime || task.startTime);
        
        if (!task.endTime) {
            endDate.setMinutes(endDate.getMinutes() + (task.duration || 60));
        }
        
        if (endDate <= startDate) {
            endDate = new Date(startDate);
            endDate.setHours(endDate.getHours() + 1);
        }
        
        const productInitial = task.product ? task.product.replace('Product ', '').toLowerCase() : '';
        
        let customClass = productInitial ? `gantt-prod-${productInitial}` : '';
        if (task.isCritical) customClass += ' gantt-critical';
        if (task.isLatePartTask) customClass += ' gantt-late-part';
        if (task.isReworkTask) customClass += ' gantt-rework';
        
        const depString = dependencyMap[task.taskId]
            .map(d => d.taskId)
            .filter(id => id && taskMap[id])
            .join(',');
        
        const ganttTask = {
            id: task.taskId,
            name: `${task.taskId} - ${task.type || 'Task'}`,
            start: startDate.toISOString().split('T')[0],
            end: endDate.toISOString().split('T')[0],
            progress: task.progress || 0,
            dependencies: depString,
            custom_class: customClass,
            // Store complete task data INCLUDING the mapped dependencies
            _task_data: {
                ...task,
                predecessors: dependencyMap[task.taskId] || [],
                successors: successorMap[task.taskId] || []
            }
        };
        
        // Debug specific tasks
        if (task.taskId === 'D_164' || task.taskId === 'C_407') {
            console.log(`Task ${task.taskId} final mapping:`, {
                original_deps: task.dependencies,
                mapped_predecessors: ganttTask._task_data.predecessors,
                mapped_successors: ganttTask._task_data.successors,
                dep_string: depString
            });
        }
        
        return ganttTask;
    });
}




// Add a debug function to check task dependencies
function debugTaskDependencies(taskId) {
    const task = ganttTasks.find(t => t.id === taskId);
    if (!task) {
        console.log(`Task ${taskId} not found`);
        return;
    }
    
    console.log(`=== Debug info for task ${taskId} ===`);
    console.log('Task object:', task);
    console.log('Task data:', task._task_data);
    console.log('Predecessors:', task._task_data.predecessors);
    console.log('Successors:', task._task_data.successors);
    console.log('Original dependencies:', task._task_data.dependencies);
    
    // Check if the dependencies exist in the task map
    if (task._task_data.predecessors && task._task_data.predecessors.length > 0) {
        console.log('Checking if predecessor tasks exist:');
        task._task_data.predecessors.forEach(pred => {
            const exists = ganttTasks.find(t => t.id === pred.taskId);
            console.log(`  ${pred.taskId}: ${exists ? 'EXISTS' : 'MISSING'}`);
        });
    }
    
    return task._task_data;
}

// Create enhanced task popup with predecessor/successor information (SINGLE CLEAN VERSION)
// Replace the createEnhancedTaskPopup function with this fixed version
// Replace the createEnhancedTaskPopup function with this fixed version
function createEnhancedTaskPopup(task) {
    const taskData = task._task_data || {};
    
    // Build HTML with proper data extraction
    let html = '<div class="gantt-popup" style="padding: 0; width: 400px; font-family: -apple-system, BlinkMacSystemFont, \'Segoe UI\', Roboto, sans-serif;">';
    
    // Header with task ID and indicators
    html += '<div style="background: #f3f4f6; padding: 12px; border-bottom: 1px solid #e5e7eb;">';
    html += `<div style="font-size: 16px; font-weight: bold; color: #1f2937;">${taskData.product || 'Unknown'} - Task ${task.id}</div>`;
    html += '<div style="margin-top: 4px;">';
    
    // Add special indicators based on task properties
    if (taskData.isCritical) {
        html += '<span style="display: inline-block; padding: 3px 8px; background: #3b82f6; color: white; border-radius: 4px; font-size: 11px; margin-right: 4px; font-weight: 600;">⚡ CRITICAL PATH</span>';
    }
    if (taskData.isLatePartTask || taskData.task_type === 'Late Part') {
        html += '<span style="display: inline-block; padding: 3px 8px; background: #f59e0b; color: white; border-radius: 4px; font-size: 11px; margin-right: 4px; font-weight: 600;">📦 LATE PART</span>';
    }
    if (taskData.isReworkTask || taskData.task_type === 'Rework') {
        html += '<span style="display: inline-block; padding: 3px 8px; background: #ef4444; color: white; border-radius: 4px; font-size: 11px; margin-right: 4px; font-weight: 600;">🔧 REWORK</span>';
    }
    html += '</div></div>';
    
    // Main content section
    html += '<div style="padding: 14px; background: white;">';
    
    // Basic task information in a clean table
    html += '<div style="margin-bottom: 14px;">';
    html += '<table style="width: 100%; font-size: 13px; border-collapse: collapse;">';
    
    // Type row
    html += '<tr>';
    html += '<td style="padding: 5px 0; color: #6b7280; width: 110px; font-weight: 500;">Type:</td>';
    html += `<td style="padding: 5px 0; color: #1f2937; font-weight: 600;">${taskData.type || taskData.task_type || 'Production'}</td>`;
    html += '</tr>';
    
    // Team row
    html += '<tr>';
    html += '<td style="padding: 5px 0; color: #6b7280; font-weight: 500;">Team:</td>';
    html += `<td style="padding: 5px 0; color: #1f2937; font-weight: 600;">${taskData.team || 'Unassigned'}</td>`;
    html += '</tr>';
    
    // Duration row
    html += '<tr>';
    html += '<td style="padding: 5px 0; color: #6b7280; font-weight: 500;">Duration:</td>';
    html += `<td style="padding: 5px 0; color: #1f2937; font-weight: 600;">${taskData.duration || 0} minutes</td>`;
    html += '</tr>';
    
    // Mechanics row
    html += '<tr>';
    html += '<td style="padding: 5px 0; color: #6b7280; font-weight: 500;">Mechanics:</td>';
    html += `<td style="padding: 5px 0; color: #1f2937; font-weight: 600;">${taskData.mechanics || taskData.mechanics_required || 1} required</td>`;
    html += '</tr>';
    
    // Slack time row with color coding
    html += '<tr>';
    html += '<td style="padding: 5px 0; color: #6b7280; font-weight: 500;">Slack:</td>';
    html += '<td style="padding: 5px 0;">';
    
    if (taskData.slack !== undefined && taskData.slack !== null) {
        const slackHours = parseFloat(taskData.slack);
        if (slackHours < 0) {
            html += `<span style="color: #ef4444; font-weight: bold;">⚠️ ${Math.abs(slackHours).toFixed(1)} hours behind</span>`;
        } else if (slackHours === 0) {
            html += `<span style="color: #f59e0b; font-weight: bold;">⚡ Critical (0 hours)</span>`;
        } else if (slackHours < 8) {
            html += `<span style="color: #f59e0b; font-weight: bold;">⏱️ ${slackHours.toFixed(1)} hours</span>`;
        } else {
            html += `<span style="color: #10b981; font-weight: 600;">✓ ${slackHours.toFixed(1)} hours</span>`;
        }
    } else {
        html += '<span style="color: #9ca3af;">N/A</span>';
    }
    html += '</td></tr>';
    
    // On-dock date for late parts
    if ((taskData.isLatePartTask || taskData.task_type === 'Late Part') && taskData.onDockDate) {
        const onDockDate = new Date(taskData.onDockDate);
        html += '<tr>';
        html += '<td style="padding: 5px 0; color: #6b7280; font-weight: 500;">On-Dock Date:</td>';
        html += `<td style="padding: 5px 0; color: #1f2937; font-weight: 600;">📅 ${onDockDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</td>`;
        html += '</tr>';
    }
    
    // Start and End times
    if (taskData.startTime) {
        const startDate = new Date(taskData.startTime);
        html += '<tr>';
        html += '<td style="padding: 5px 0; color: #6b7280; font-weight: 500;">Start:</td>';
        html += `<td style="padding: 5px 0; color: #1f2937; font-weight: 600;">${startDate.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true })}</td>`;
        html += '</tr>';
    }
    
    if (taskData.endTime) {
        const endDate = new Date(taskData.endTime);
        html += '<tr>';
        html += '<td style="padding: 5px 0; color: #6b7280; font-weight: 500;">End:</td>';
        html += `<td style="padding: 5px 0; color: #1f2937; font-weight: 600;">${endDate.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true })}</td>`;
        html += '</tr>';
    }
    
    html += '</table></div>';
    
    // Predecessors section with better styling
    html += '<div style="border-top: 1px solid #e5e7eb; padding-top: 12px; margin-top: 12px;">';
    if (taskData.predecessors && taskData.predecessors.length > 0) {
        html += '<div style="font-size: 13px; font-weight: 600; color: #374151; margin-bottom: 8px;">⬅️ Predecessors (must complete before this task):</div>';
        html += '<div style="background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 6px; padding: 10px;">';
        
        taskData.predecessors.forEach((pred, index) => {
            if (index > 0) html += '<div style="border-top: 1px solid #e5e7eb; margin: 6px 0;"></div>';
            html += '<div style="display: grid; grid-template-columns: 1fr 1.5fr auto; gap: 10px; align-items: center; font-size: 12px; padding: 3px 0;">';
            html += `<div><strong style="color: #1f2937; font-size: 13px;">📌 ${pred.taskId || 'Unknown'}</strong></div>`;
            html += `<div style="color: #6b7280;">Team: ${pred.team || 'Not assigned'}</div>`;
            html += `<div style="color: #9ca3af; font-size: 11px; background: #f3f4f6; padding: 2px 6px; border-radius: 3px;">${pred.type || 'FS'}</div>`;
            html += '</div>';
        });
        
        html += '</div>';
    } else {
        html += '<div style="font-size: 13px; color: #10b981; background: #dcfce7; padding: 8px; border-radius: 6px;">✅ No predecessors (can start immediately)</div>';
    }
    html += '</div>';
    
    // Successors section with better styling
    html += '<div style="border-top: 1px solid #e5e7eb; padding-top: 12px; margin-top: 12px;">';
    if (taskData.successors && taskData.successors.length > 0) {
        html += '<div style="font-size: 13px; font-weight: 600; color: #374151; margin-bottom: 8px;">➡️ Successors (tasks waiting for this to complete):</div>';
        html += '<div style="background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 6px; padding: 10px;">';
        
        taskData.successors.forEach((succ, index) => {
            if (index > 0) html += '<div style="border-top: 1px solid #e5e7eb; margin: 6px 0;"></div>';
            html += '<div style="display: grid; grid-template-columns: 1fr 1.5fr auto; gap: 10px; align-items: center; font-size: 12px; padding: 3px 0;">';
            html += `<div><strong style="color: #1f2937; font-size: 13px;">📍 ${succ.taskId || 'Unknown'}</strong></div>`;
            html += `<div style="color: #6b7280;">Team: ${succ.team || 'Not assigned'}</div>`;
            html += `<div style="color: #9ca3af; font-size: 11px; background: #f3f4f6; padding: 2px 6px; border-radius: 3px;">${succ.type || 'FS'}</div>`;
            html += '</div>';
        });
        
        html += '</div>';
    } else {
        html += '<div style="font-size: 13px; color: #9ca3af; background: #f3f4f6; padding: 8px; border-radius: 6px;">🏁 No successors (final task in chain)</div>';
    }
    html += '</div>';
    
    html += '</div></div>';
    
    return html;
}

// Highlight non-work days (weekends and holidays)
function highlightNonWorkDays() {
    if (!ganttChart) return;
    
    const svg = document.querySelector('#ganttChartSvg svg');
    if (!svg) return;
    
    // Remove existing highlights
    svg.querySelectorAll('.non-work-day-highlight').forEach(el => el.remove());
    
    // Get all date headers
    const dateGroups = svg.querySelectorAll('.date');
    
    dateGroups.forEach(dateGroup => {
        const textElement = dateGroup.querySelector('text');
        if (!textElement) return;
        
        const dateText = textElement.textContent;
        if (!dateText) return;
        
        // Parse the date
        const date = parseDateFromTick(dateText);
        if (!date) return;
        
        // Check if weekend
        const dayOfWeek = date.getDay();
        if (dayOfWeek === 0 || dayOfWeek === 6) {
            // Style the text
            textElement.style.fill = '#DC2626';
            textElement.style.fontWeight = 'bold';
            
            // Get position for column highlight
            const transform = dateGroup.getAttribute('transform');
            if (transform) {
                const match = transform.match(/translate\(([^,]+),/);
                if (match) {
                    const xPos = parseFloat(match[1]);
                    highlightColumn(svg, xPos, '#FEE2E2');
                }
            }
        }
        
        // Check if holiday
        if (isHoliday(date)) {
            textElement.style.fill = '#F59E0B';
            textElement.style.fontWeight = 'bold';
            
            const transform = dateGroup.getAttribute('transform');
            if (transform) {
                const match = transform.match(/translate\(([^,]+),/);
                if (match) {
                    const xPos = parseFloat(match[1]);
                    highlightColumn(svg, xPos, '#FEF3C7');
                }
            }
        }
    });
}

// Parse date from tick text
function parseDateFromTick(dateText) {
    const currentYear = new Date().getFullYear();
    const currentMonth = new Date().getMonth();
    
    // Try parsing as day number (e.g., "25")
    if (/^\d{1,2}$/.test(dateText)) {
        const day = parseInt(dateText);
        return new Date(currentYear, currentMonth, day);
    }
    
    // Try parsing as date string
    const parsed = new Date(dateText);
    if (!isNaN(parsed.getTime())) {
        return parsed;
    }
    
    // Try parsing "Aug 25" format
    const monthDayMatch = dateText.match(/^(\w{3})\s+(\d{1,2})$/);
    if (monthDayMatch) {
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const monthIndex = months.indexOf(monthDayMatch[1]);
        if (monthIndex !== -1) {
            return new Date(currentYear, monthIndex, parseInt(monthDayMatch[2]));
        }
    }
    
    return null;
}

// Check if date is a holiday
function isHoliday(date) {
    if (!scenarioData || !scenarioData.holidays) return false;
    
    const dateStr = date.toISOString().split('T')[0];
    return scenarioData.holidays.includes(dateStr);
}

// Highlight a column in the Gantt chart
function highlightColumn(svg, xPos, color) {
    const grid = svg.querySelector('.grid');
    if (!grid) return;
    
    // Get chart dimensions
    const chartHeight = svg.getAttribute('height') || '400';
    
    // Create background rectangle
    const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    rect.setAttribute('x', xPos - 15);
    rect.setAttribute('y', 0);
    rect.setAttribute('width', 30);
    rect.setAttribute('height', chartHeight);
    rect.setAttribute('fill', color);
    rect.setAttribute('opacity', '0.3');
    rect.setAttribute('class', 'non-work-day-highlight');
    
    // Insert at the beginning to be behind everything
    grid.insertBefore(rect, grid.firstChild);
}

// Apply custom styling to Gantt chart (SINGLE CLEAN VERSION)
function applyGanttStyling() {
    // Only add styles if they don't exist
    if (document.getElementById('gantt-popup-styles')) return;
    
    const style = document.createElement('style');
    style.id = 'gantt-popup-styles';
    style.textContent = `
        /* Override default Gantt popup styles */
        .gantt-container .popup-wrapper {
            max-width: none !important;
        }
        
        .gantt-container .pointer {
            display: none !important;
        }
        
        .gantt .bar-wrapper:hover .bar {
            opacity: 0.8;
        }
        
        /* Ensure critical path is visible */
        .gantt .gantt-critical .bar {
            stroke: #000 !important;
            stroke-width: 3px !important;
        }
        
        /* Ensure product colors are applied */
        .gantt .gantt-prod-a .bar { fill: #DC2626 !important; }
        .gantt .gantt-prod-b .bar { fill: #FCD34D !important; }
        .gantt .gantt-prod-c .bar { fill: #10B981 !important; }
        .gantt .gantt-prod-d .bar { fill: #3B82F6 !important; }
        .gantt .gantt-prod-e .bar { fill: #8B5CF6 !important; }
    `;
    document.head.appendChild(style);
}

// Filter Gantt chart
function filterGanttChart() {
    const productFilter = document.getElementById('ganttProductSelect').value;
    const teamFilter = document.getElementById('ganttTeamSelect').value;
    
    if (!scenarioData || !scenarioData.tasks) {
        console.error('No scenario data available for filtering');
        return;
    }
    
    let filteredTasks = scenarioData.tasks;
    
    if (productFilter && productFilter !== 'all') {
        filteredTasks = filteredTasks.filter(t => t.product === productFilter);
    }
    
    if (teamFilter && teamFilter !== 'all') {
        filteredTasks = filteredTasks.filter(t => t.team === teamFilter);
    }
    
    // Re-prepare tasks and refresh chart
    ganttTasks = prepareGanttTasks(filteredTasks);
    
    if (ganttChart && ganttTasks.length > 0) {
        // Refresh the chart with filtered tasks
        const ganttContainer = document.getElementById('ganttChartSvg');
        if (ganttContainer) {
            ganttContainer.innerHTML = '';
            ganttChart = new Gantt("#ganttChartSvg", ganttTasks, {
                view_mode: ganttChart.get_view_mode ? ganttChart.get_view_mode() : 'Day',
                date_format: 'YYYY-MM-DD',
                custom_popup_html: function(task) {
                    return createEnhancedTaskPopup(task);
                }
            });
            setTimeout(() => {
                applyGanttStyling();
                highlightNonWorkDays();
            }, 100);
        }
    } else if (ganttTasks.length === 0) {
        document.getElementById('ganttChartSvg').innerHTML = '<p style="text-align: center; padding: 40px;">No tasks match the selected filters</p>';
    }
}


// Sort Gantt chart
function sortGanttChart() {
    const sortBy = document.getElementById('ganttSortSelect').value;
    
    if (!ganttTasks || ganttTasks.length === 0) return;
    
    switch(sortBy) {
        case 'product-asc':
            ganttTasks.sort((a, b) => {
                const prodA = a._task_data.product || '';
                const prodB = b._task_data.product || '';
                return prodA.localeCompare(prodB);
            });
            break;
        case 'product-desc':
            ganttTasks.sort((a, b) => {
                const prodA = a._task_data.product || '';
                const prodB = b._task_data.product || '';
                return prodB.localeCompare(prodA);
            });
            break;
        case 'team-asc':
            ganttTasks.sort((a, b) => {
                const teamA = a._task_data.team || '';
                const teamB = b._task_data.team || '';
                return teamA.localeCompare(teamB);
            });
            break;
        case 'team-desc':
            ganttTasks.sort((a, b) => {
                const teamA = a._task_data.team || '';
                const teamB = b._task_data.team || '';
                return teamB.localeCompare(teamA);
            });
            break;
        case 'slack-asc':
            ganttTasks.sort((a, b) => {
                const slackA = a._task_data.slack || 999;
                const slackB = b._task_data.slack || 999;
                return slackA - slackB;
            });
            break;
        case 'slack-desc':
            ganttTasks.sort((a, b) => {
                const slackA = a._task_data.slack || 0;
                const slackB = b._task_data.slack || 0;
                return slackB - slackA;
            });
            break;
        case 'type-asc':
            ganttTasks.sort((a, b) => {
                const typeA = a._task_data.type || '';
                const typeB = b._task_data.type || '';
                return typeA.localeCompare(typeB);
            });
            break;
        default:
            // Default sort by start time
            ganttTasks.sort((a, b) => new Date(a.start) - new Date(b.start));
    }
    
    if (ganttChart) {
        ganttChart.refresh(ganttTasks);
        setTimeout(() => highlightNonWorkDays(), 100);
    }
}

// Change Gantt view mode
function changeGanttView(mode) {
    if (ganttChart) {
        ganttChart.change_view_mode(mode);
        
        // Update button states
        document.querySelectorAll('.gantt-view-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        event.target.classList.add('active');
        
        // Re-apply highlighting
        setTimeout(() => highlightNonWorkDays(), 100);
    }
}

// Reset Gantt filters
function resetGanttFilters() {
    document.getElementById('ganttProductSelect').value = 'all';
    document.getElementById('ganttTeamSelect').value = 'all';
    document.getElementById('ganttSortSelect').value = 'default';
    
    // Re-initialize chart with all tasks
    initializeGanttChart();
}

// Show product details
async function showProductDetails(productName) {
    try {
        const response = await fetch(`/api/product/${productName}/tasks?scenario=${currentScenario}`);
        const data = await response.json();
        
        if (response.ok) {
            console.log(`Product ${productName} details:`, data);
            
            // Create a more detailed alert or modal
            let message = `${productName} Details:\n\n`;
            message += `Total Tasks: ${data.totalTasks}\n`;
            message += `\nTask Breakdown:\n`;
            
            if (data.taskBreakdown) {
                for (const [type, count] of Object.entries(data.taskBreakdown)) {
                    message += `  • ${type}: ${count}\n`;
                }
            }
            
            if (data.criticalTasks) {
                message += `\nCritical Path Tasks: ${data.criticalTasks}\n`;
            }
            
            if (data.completionDate) {
                message += `\nProjected Completion: ${new Date(data.completionDate).toLocaleDateString()}\n`;
            }
            
            alert(message);
        }
    } catch (error) {
        console.error('Error loading product details:', error);
    }
}

// Run priority simulation
async function runPrioritySimulation() {
    const product = document.getElementById('priorityProduct').value;
    const level = document.getElementById('priorityLevel').value;
    const days = document.getElementById('simDays').value;
    
    if (!product) {
        alert('Please select a product to prioritize');
        return;
    }
    
    const resultsDiv = document.getElementById('simulationResults');
    resultsDiv.style.display = 'block';
    resultsDiv.innerHTML = '<div class="loading">Running simulation...</div>';
    
    try {
        const response = await fetch('/api/simulate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                scenario: currentScenario,
                product: product,
                level: level,
                days: parseInt(days)
            })
        });
        
        const result = await response.json();
        
        if (response.ok) {
            displaySimulationResults(result);
        } else {
            resultsDiv.innerHTML = `<div style="color: red;">Simulation failed: ${result.error}</div>`;
        }
    } catch (error) {
        resultsDiv.innerHTML = `<div style="color: red;">Error: ${error.message}</div>`;
    }
}

// Display simulation results
function displaySimulationResults(result) {
    const resultsDiv = document.getElementById('simulationResults');
    
    let html = '<h4>Simulation Results</h4>';
    html += '<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 15px;">';
    
    // Before column
    html += '<div>';
    html += '<h5>Current State</h5>';
    html += '<ul style="font-size: 13px;">';
    result.before.forEach(product => {
        const status = product.onTime ? '✓' : '✗';
        const days = product.latenessDays > 0 ? `+${product.latenessDays}` : product.latenessDays;
        html += `<li>${status} ${product.name}: ${days} days</li>`;
    });
    html += '</ul>';
    html += '</div>';
    
    // After column
    html += '<div>';
    html += '<h5>After Prioritization</h5>';
    html += '<ul style="font-size: 13px;">';
    result.after.forEach(product => {
        const status = product.onTime ? '✓' : '✗';
        const days = product.latenessDays > 0 ? `+${product.latenessDays}` : product.latenessDays;
        const change = product.latenessDays - 
                      result.before.find(p => p.name === product.name).latenessDays;
        const changeStr = change !== 0 ? ` (${change > 0 ? '+' : ''}${change})` : '';
        html += `<li>${status} ${product.name}: ${days} days${changeStr}</li>`;
    });
    html += '</ul>';
    html += '</div>';
    
    html += '</div>';
    
    // Impact summary
    const impactLevel = result.impactScore > 50 ? 'high-impact' : 
                       result.impactScore > 20 ? 'medium-impact' : 'low-impact';
    
    html += `<div class="impact-result ${impactLevel}" style="margin-top: 15px; padding: 10px; border-radius: 6px;">`;
    html += `<strong>Impact Score: ${result.impactScore}/100</strong><br>`;
    html += `<span style="font-size: 12px;">${result.recommendation}</span>`;
    html += '</div>';
    
    resultsDiv.innerHTML = html;
}

// Helper functions
function getTaskTypeClass(type) {
    const typeMap = {
        'Production': 'production',
        'Quality Inspection': 'quality',
        'Late Part': 'late-part',
        'Rework': 'rework'
    };
    return typeMap[type] || 'production';
}

function getTaskTypeColor(type) {
    const colorMap = {
        'Production': '#10b981',
        'Quality Inspection': '#3b82f6',
        'Late Part': '#f59e0b',
        'Rework': '#ef4444'
    };
    return colorMap[type] || '#6b7280';
}

function formatTime(date) {
    return date.toLocaleTimeString('en-US', { 
        hour: 'numeric', 
        minute: '2-digit', 
        hour12: true 
    });
}

function formatDateTime(date) {
    return date.toLocaleString('en-US', { 
        month: 'short',
        day: 'numeric',
        hour: 'numeric', 
        minute: '2-digit', 
        hour12: true 
    });
}

function formatDate(date) {
    return date.toLocaleDateString('en-US', { 
        month: 'short', 
        day: 'numeric',
        year: 'numeric'
    });
}

function formatSlackTime(slackHours) {
    if (!slackHours && slackHours !== 0) return 'N/A';
    
    if (slackHours < 0) {
        return `<span style="color: #EF4444; font-weight: bold;">${Math.abs(slackHours).toFixed(1)} hours late</span>`;
    } else if (slackHours < 8) {
        return `<span style="color: #F59E0B; font-weight: bold;">${slackHours.toFixed(1)} hours</span>`;
    } else {
        return `<span style="color: #10B981;">${slackHours.toFixed(1)} hours</span>`;
    }
}

// Loading and error states
function showLoading(message = 'Loading...') {
    const content = document.querySelector('.main-content');
    if (content) {
        const loadingDiv = document.createElement('div');
        loadingDiv.id = 'loadingIndicator';
        loadingDiv.className = 'loading';
        loadingDiv.innerHTML = `
            <div style="text-align: center;">
                <div class="spinner"></div>
                <div style="margin-top: 20px;">${message}</div>
            </div>
        `;
        content.appendChild(loadingDiv);
    }
}

function hideLoading() {
    const loadingDiv = document.getElementById('loadingIndicator');
    if (loadingDiv) {
        loadingDiv.remove();
    }
}

function showError(message) {
    const content = document.querySelector('.main-content');
    if (content) {
        content.innerHTML = `
            <div style="text-align: center; padding: 40px; color: #ef4444;">
                <h2>Error</h2>
                <p>${message}</p>
                <button onclick="location.reload()" class="btn btn-primary" style="margin-top: 20px;">
                    Reload Page
                </button>
            </div>
        `;
    }
}

// Auto-assign function
async function autoAssign() {
    const selects = document.querySelectorAll('.assign-select');
    const mechanics = ['mech1', 'mech2', 'mech3', 'mech4'];
    let mechanicIndex = 0;
    let assignmentCount = 0;
    
    for (const select of selects) {
        const taskId = select.dataset.taskId;
        const mechanicId = mechanics[mechanicIndex % mechanics.length];
        
        try {
            const response = await fetch('/api/assign_task', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    taskId: taskId,
                    mechanicId: mechanicId,
                    scenario: currentScenario
                })
            });
            
            if (response.ok) {
                select.value = mechanicId;
                assignmentCount++;
            }
        } catch (error) {
            console.error('Error assigning task:', error);
        }
        
        mechanicIndex++;
    }
    
    alert(`Successfully assigned ${assignmentCount} tasks to mechanics!`);
}

// Export tasks function
function exportTasks() {
    window.location.href = `/api/export/${currentScenario}`;
}

// Refresh data
async function refreshData() {
    if (confirm('This will recalculate all scenarios. It may take a few minutes. Continue?')) {
        showLoading('Refreshing all scenarios...');
        
        try {
            const response = await fetch('/api/refresh', { method: 'POST' });
            const result = await response.json();
            
            if (result.success) {
                await loadAllScenarios();
                alert('All scenarios refreshed successfully!');
            } else {
                alert('Failed to refresh: ' + result.error);
            }
        } catch (error) {
            alert('Error refreshing data: ' + error.message);
        } finally {
            hideLoading();
        }
    }
}