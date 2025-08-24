# app.py - Enhanced Flask Web Server with Async Data Loading
# Compatible with Product-Task Instance Model where tasks are instantiated per product

from flask import Flask, render_template, jsonify, request, send_file
from flask_cors import CORS
import pandas as pd
import json
from datetime import datetime, timedelta
import os
from collections import defaultdict
import traceback
import threading
import time

# Import your enhanced scheduler
from scheduler import ProductionScheduler

app = Flask(__name__)
CORS(app)  # Enable CORS for API calls

# Global scheduler instance and state
scheduler = None
scenario_results = {}
initialization_status = {
    'initialized': False,
    'initializing': False,
    'error': None,
    'progress': 0,
    'current_scenario': None,
    'scenarios_completed': [],
    'start_time': None,
    'end_time': None
}
initialization_lock = threading.Lock()


def initialize_scheduler_async():
    """Initialize the scheduler and run all scenarios in a background thread"""
    global scheduler, scenario_results, initialization_status

    with initialization_lock:
        if initialization_status['initializing']:
            return  # Already running
        initialization_status['initializing'] = True
        initialization_status['start_time'] = datetime.now()
        initialization_status['progress'] = 0
        initialization_status['scenarios_completed'] = []

    try:
        print("=" * 80)
        print("Starting Background Scheduler Initialization")
        print("=" * 80)

        # Initialize scheduler with enhanced features
        initialization_status['current_scenario'] = 'Loading data'
        scheduler = ProductionScheduler('scheduling_data.csv', debug=False, late_part_delay_days=1.0)
        scheduler.load_data_from_csv()

        print("\nScheduler loaded successfully!")
        print(f"Total task instances: {len(scheduler.tasks)}")
        print(f"Products: {len(scheduler.delivery_dates)}")

        initialization_status['progress'] = 20

        # Show task instance breakdown
        instance_counts = defaultdict(int)
        for task_id in scheduler.tasks:
            product, task_num = scheduler.parse_product_task_id(task_id)
            if product:
                instance_counts[product] += 1

        print("\nTask instances per product:")
        for product in sorted(instance_counts.keys()):
            print(f"  {product}: {instance_counts[product]} instances")

        print(f"\nLate parts: {len(scheduler.late_part_tasks)}")
        print(f"Rework tasks: {len(scheduler.rework_tasks)}")

        # Run baseline scenario
        print("\n" + "-" * 40)
        print("Running BASELINE scenario...")
        initialization_status['current_scenario'] = 'Baseline'
        scheduler.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)
        scenario_results['baseline'] = export_scenario_data(scheduler, 'baseline')
        initialization_status['scenarios_completed'].append('baseline')
        initialization_status['progress'] = 40
        print(f"✓ Baseline complete: {scenario_results['baseline']['makespan']} days makespan")

        # Run Scenario 1
        print("\nRunning SCENARIO 1 (CSV Capacity)...")
        initialization_status['current_scenario'] = 'Scenario 1'
        result1 = scheduler.scenario_1_csv_headcount()
        scenario_results['scenario1'] = export_scenario_data(scheduler, 'scenario1', result1)
        initialization_status['scenarios_completed'].append('scenario1')
        initialization_status['progress'] = 60
        print(f"✓ Scenario 1 complete: {scenario_results['scenario1']['makespan']} days makespan")

        # Run Scenario 2 - Just-In-Time Optimization
        print("\nRunning SCENARIO 2 (Just-In-Time Optimization)...")
        initialization_status['current_scenario'] = 'Scenario 2'
        result2 = scheduler.scenario_2_just_in_time_optimization(
            min_mechanics=1, max_mechanics=30,
            min_quality=1, max_quality=10,
            target_lateness=-1,  # Target 1 day early
            tolerance=2  # Accept within 2 days of target
        )
        if result2:
            scenario_results['scenario2'] = export_scenario_data(scheduler, 'scenario2', result2)
            initialization_status['scenarios_completed'].append('scenario2')
            print(f"✓ Scenario 2 complete: {scenario_results['scenario2']['makespan']} days makespan")
            if 'targetLateness' in scenario_results['scenario2']:
                print(f"  Target: {abs(scenario_results['scenario2']['targetLateness'])} day(s) early")
                print(f"  Max deviation: {scenario_results['scenario2'].get('maxDeviation', 0):.1f} days")
        else:
            print("✗ Scenario 2 failed to find solution meeting target")
            scenario_results['scenario2'] = create_failed_scenario_data()
        initialization_status['progress'] = 80

        # Run Scenario 3 - Enhanced Multi-dimensional with minimum lateness
        print("\nRunning SCENARIO 3 (Multi-Dimensional Optimization)...")
        initialization_status['current_scenario'] = 'Scenario 3'
        result3 = scheduler.scenario_3_multidimensional_optimization(
            min_mechanics=1, max_mechanics=30,
            min_quality=1, max_quality=15,
            max_iterations=100  # Reduced for faster dashboard loading
        )
        if result3:
            scenario_results['scenario3'] = export_scenario_data(scheduler, 'scenario3', result3)
            initialization_status['scenarios_completed'].append('scenario3')
            print(f"✓ Scenario 3 complete: {scenario_results['scenario3']['makespan']} days makespan")
            if 'maxLateness' in scenario_results['scenario3']:
                print(f"  Maximum lateness: {scenario_results['scenario3']['maxLateness']} days")
        else:
            print("✗ Scenario 3 failed to find solution")
            scenario_results['scenario3'] = create_failed_scenario_data()
        initialization_status['progress'] = 100

        print("\n" + "=" * 80)
        print("All scenarios completed successfully!")
        print("=" * 80)

        initialization_status['initialized'] = True
        initialization_status['initializing'] = False
        initialization_status['end_time'] = datetime.now()
        initialization_status['current_scenario'] = None

        return scenario_results

    except Exception as e:
        print(f"\n✗ ERROR during initialization: {str(e)}")
        traceback.print_exc()
        initialization_status['error'] = str(e)
        initialization_status['initializing'] = False
        initialization_status['initialized'] = False
        initialization_status['current_scenario'] = None
        raise


def initialize_scheduler_lazy():
    """Initialize scheduler on first request (lazy loading)"""
    global scheduler, scenario_results, initialization_status

    with initialization_lock:
        if initialization_status['initialized'] or initialization_status['initializing']:
            return

        # Start initialization in background thread
        thread = threading.Thread(target=initialize_scheduler_async, daemon=True)
        thread.start()


def export_scenario_data(scheduler, scenario_name, result=None):
    """Export scenario data in format needed by dashboard with product-task instance support"""

    # Get metrics
    metrics = scheduler.calculate_lateness_metrics()
    makespan = scheduler.calculate_makespan()

    # Determine the correct capacities for this scenario
    if scenario_name == 'scenario2' and result and 'config' in result:
        team_capacities_to_use = result['config'].get('mechanic', scheduler.team_capacity)
        quality_capacities_to_use = result['config'].get('quality', scheduler.quality_team_capacity)
    elif scenario_name == 'scenario3' and result and 'config' in result:
        team_capacities_to_use = result['config'].get('mechanic', scheduler.team_capacity)
        quality_capacities_to_use = result['config'].get('quality', scheduler.quality_team_capacity)
    else:
        team_capacities_to_use = dict(scheduler._original_team_capacity)
        quality_capacities_to_use = dict(scheduler._original_quality_capacity)

    # Calculate utilization with the CORRECT capacities for this scenario
    utilization_data = calculate_team_utilization(scheduler, team_capacities_to_use, quality_capacities_to_use)

    # Format tasks for dashboard with product-task instance IDs
    tasks = []
    for task_data in scheduler.global_priority_list[:1000]:  # Export top 1000 tasks
        # Parse product-task ID
        product, task_num = scheduler.parse_product_task_id(task_data['task_id'])

        # Create display name that clearly shows product-task instance
        if task_data['task_type'] == 'Quality Inspection':
            if task_num and task_num > 10000:
                original_task_num = task_num - 10000
                display_name = f"{product} QI-{original_task_num}"
            else:
                display_name = f"{product} {task_data['display_name']}"
        elif task_data['task_type'] == 'Late Part':
            display_name = f"{product} LP-{task_num}"
        elif task_data['task_type'] == 'Rework':
            display_name = f"{product} RW-{task_num}"
        else:
            display_name = f"{product} T-{task_num}"

        # Calculate if task is critical based on multiple factors
        is_critical = False

        # Check slack time - tasks with less than 24 hours slack are critical
        slack_hours = task_data.get('slack_hours', float('inf'))
        if slack_hours < 24 and slack_hours > -999999:
            is_critical = True

        # Late parts and rework are always critical
        if task_data['task_id'] in scheduler.late_part_tasks or task_data['task_id'] in scheduler.rework_tasks:
            is_critical = True

        # Tasks near the end of the schedule are critical
        if task_data.get('scheduled_end') and product:
            delivery_date = scheduler.delivery_dates.get(product, None)
            if delivery_date:
                days_to_delivery = (delivery_date - task_data['scheduled_end']).days
                if days_to_delivery <= 2:  # Within 2 days of delivery
                    is_critical = True

        # ENHANCED: Check for ALL types of dependencies
        dependencies = []

        # 1. Check baseline precedence constraints
        for constraint in scheduler.precedence_constraints:
            if constraint['Second'] == task_data['task_id']:
                dep_product, dep_task_num = scheduler.parse_product_task_id(constraint['First'])
                dependencies.append({
                    'type': 'Baseline',
                    'taskId': constraint['First'],  # Full task ID for Gantt
                    'task': constraint['First'],
                    'taskNum': dep_task_num,
                    'product': dep_product or constraint.get('Product', 'Unknown'),
                    'relationship': constraint.get('Relationship', 'Finish <= Start')
                })

        # 2. Check late part constraints
        for lp_constraint in scheduler.late_part_constraints:
            if lp_constraint['Second'] == task_data['task_id']:
                dep_product, dep_task_num = scheduler.parse_product_task_id(lp_constraint['First'])
                dependencies.append({
                    'type': 'Late Part',
                    'taskId': lp_constraint['First'],
                    'task': lp_constraint['First'],
                    'taskNum': dep_task_num,
                    'product': dep_product or lp_constraint.get('Product_Line', 'Unknown'),
                    'relationship': lp_constraint.get('Relationship', 'Finish <= Start')
                })

        # 3. Check rework constraints
        for rw_constraint in scheduler.rework_constraints:
            if rw_constraint['Second'] == task_data['task_id']:
                dep_product, dep_task_num = scheduler.parse_product_task_id(rw_constraint['First'])
                dependencies.append({
                    'type': 'Rework',
                    'taskId': rw_constraint['First'],
                    'task': rw_constraint['First'],
                    'taskNum': dep_task_num,
                    'product': dep_product or rw_constraint.get('Product_Line', 'Unknown'),
                    'relationship': rw_constraint.get('Relationship', 'Finish <= Start')
                })

        # 4. Check quality inspection relationships
        if task_data['task_type'] == 'Quality Inspection' and task_data['task_id'] in scheduler.quality_inspections:
            primary_task = scheduler.quality_inspections[task_data['task_id']].get('primary_task')
            if primary_task:
                dep_product, dep_task_num = scheduler.parse_product_task_id(primary_task)
                dependencies.append({
                    'type': 'Quality',
                    'taskId': primary_task,
                    'task': primary_task,
                    'taskNum': dep_task_num,
                    'product': dep_product or 'Unknown',
                    'relationship': 'Finish = Start'
                })

        # Find successor tasks
        successors = []

        # If task has QI, add that as a successor
        if task_data['task_id'] in scheduler.quality_requirements:
            qi_task = scheduler.quality_requirements[task_data['task_id']]
            succ_product, succ_task_num = scheduler.parse_product_task_id(qi_task)
            successors.append({
                'type': 'Quality',
                'taskId': qi_task,
                'task': qi_task,
                'taskNum': succ_task_num,
                'product': succ_product or 'Unknown',
                'relationship': 'Finish = Start'
            })

        # Find baseline tasks that depend on this one
        for constraint in scheduler.precedence_constraints:
            if constraint['First'] == task_data['task_id']:
                succ_product, succ_task_num = scheduler.parse_product_task_id(constraint['Second'])
                successors.append({
                    'type': 'Baseline',
                    'taskId': constraint['Second'],
                    'task': constraint['Second'],
                    'taskNum': succ_task_num,
                    'product': succ_product or constraint.get('Product', 'Unknown'),
                    'relationship': constraint.get('Relationship', 'Finish <= Start')
                })

        # Find late part tasks that depend on this one
        for lp_constraint in scheduler.late_part_constraints:
            if lp_constraint['First'] == task_data['task_id']:
                succ_product, succ_task_num = scheduler.parse_product_task_id(lp_constraint['Second'])
                successors.append({
                    'type': 'Late Part Dependent',
                    'taskId': lp_constraint['Second'],
                    'task': lp_constraint['Second'],
                    'taskNum': succ_task_num,
                    'product': succ_product or lp_constraint.get('Product_Line', 'Unknown'),
                    'relationship': lp_constraint.get('Relationship', 'Finish <= Start')
                })

        # Find rework tasks that depend on this one
        for rw_constraint in scheduler.rework_constraints:
            if rw_constraint['First'] == task_data['task_id']:
                succ_product, succ_task_num = scheduler.parse_product_task_id(rw_constraint['Second'])
                successors.append({
                    'type': 'Rework Dependent',
                    'taskId': rw_constraint['Second'],
                    'task': rw_constraint['Second'],
                    'taskNum': succ_task_num,
                    'product': succ_product or rw_constraint.get('Product_Line', 'Unknown'),
                    'relationship': rw_constraint.get('Relationship', 'Finish <= Start')
                })

        tasks.append({
            'priority': task_data['global_priority'],
            'taskId': task_data['task_id'],
            'taskNum': task_num,
            'type': task_data['task_type'],
            'displayName': display_name,
            'product': product or task_data['product_line'],
            'team': task_data['team'],
            'startTime': task_data['scheduled_start'].isoformat(),
            'endTime': task_data['scheduled_end'].isoformat(),
            'duration': task_data['duration_minutes'],
            'mechanics': task_data['mechanics_required'],
            'shift': task_data['shift'],
            'slackHours': round(slack_hours, 1) if slack_hours < 999999 else None,
            'dependencies': dependencies,
            'successors': successors,
            'isLatePartTask': task_data['task_id'] in scheduler.late_part_tasks,
            'isReworkTask': task_data['task_id'] in scheduler.rework_tasks,
            'isCritical': is_critical,
            'onDockDate': scheduler.on_dock_dates.get(task_data['task_id'], '').isoformat()
            if task_data['task_id'] in scheduler.on_dock_dates else None
        })

    # Format products for dashboard with enhanced metrics
    products = []
    for product_name, delivery_date in scheduler.delivery_dates.items():
        product_metrics = metrics.get(product_name, {})

        # Get product-specific tasks
        product_tasks = [t for t in scheduler.global_priority_list
                         if t['product_line'] == product_name]

        # Count task types for this product
        task_type_counts = defaultdict(int)
        unique_task_nums = set()
        critical_task_count = 0
        scheduled_finish = None

        for task in product_tasks:
            task_type_counts[task['task_type']] += 1
            if task.get('task_num'):
                unique_task_nums.add(task['task_num'])

            # Find latest task end time (scheduled finish)
            if task.get('scheduled_end'):
                if scheduled_finish is None or task['scheduled_end'] > scheduled_finish:
                    scheduled_finish = task['scheduled_end']

            # Count critical tasks
            task_slack = task.get('slack_hours', float('inf'))
            if task_slack < 24 and task_slack > -999999:
                critical_task_count += 1

            # Late parts and rework are always critical
            if task['task_id'] in scheduler.late_part_tasks:
                critical_task_count += 1
            elif task['task_id'] in scheduler.rework_tasks:
                critical_task_count += 1

        # Count late parts and rework specifically for this product
        late_parts_count = sum(1 for task in product_tasks if task['task_id'] in scheduler.late_part_tasks)
        rework_count = sum(1 for task in product_tasks if task['task_id'] in scheduler.rework_tasks)

        # Calculate progress
        progress = 0
        if product_tasks:
            total_duration = sum(t['duration_minutes'] for t in product_tasks)
            # Simple progress estimate based on schedule
            first_task_start = min(t['scheduled_start'] for t in product_tasks)
            last_task_end = max(t['scheduled_end'] for t in product_tasks)
            total_span = (last_task_end - first_task_start).total_seconds() / 60

            # Estimate progress based on current time
            now = datetime.now()
            if now < first_task_start:
                progress = 0
            elif now > last_task_end:
                progress = 100
            else:
                elapsed = (now - first_task_start).total_seconds() / 60
                progress = min(100, int((elapsed / total_span) * 100)) if total_span > 0 else 0

        # Determine on-time status
        on_time = False
        lateness_days = 0
        if scheduled_finish and delivery_date:
            lateness_days = (scheduled_finish - delivery_date).days
            on_time = lateness_days <= 0
        elif product_metrics.get('lateness_days') is not None:
            lateness_days = product_metrics['lateness_days']
            on_time = product_metrics.get('on_time', False)

        products.append({
            'name': product_name,
            'deliveryDate': delivery_date.isoformat(),
            'scheduledFinish': scheduled_finish.isoformat() if scheduled_finish else None,
            'onTime': on_time,
            'latenessDays': lateness_days,
            'totalTasks': product_metrics.get('total_tasks', len(product_tasks)),
            'uniqueTasks': len(unique_task_nums),
            'progress': progress,
            'daysRemaining': (delivery_date - datetime.now()).days,
            'criticalTasks': critical_task_count,
            'criticalPath': critical_task_count,  # Backward compatibility
            'latePartsCount': late_parts_count,
            'reworkCount': rework_count,
            'taskBreakdown': dict(task_type_counts)
        })

    # Build team capacities dictionary for the dashboard
    team_capacities = {}
    for team, capacity in team_capacities_to_use.items():
        team_capacities[team] = capacity
    for team, capacity in quality_capacities_to_use.items():
        team_capacities[team] = capacity

    # Calculate summary metrics
    on_time_products = sum(1 for p in products if p['onTime'])
    on_time_rate = int((on_time_products / len(products) * 100)) if products else 0

    avg_utilization = int(sum(utilization_data.values()) / len(utilization_data)) if utilization_data else 0

    # Calculate total workforce using the scenario-specific capacities
    total_workforce = sum(team_capacities_to_use.values()) + sum(quality_capacities_to_use.values())

    # Count task types
    task_type_summary = defaultdict(int)
    for task in tasks:
        task_type_summary[task['type']] += 1

    # Get lateness metrics
    max_lateness = max((m['latenessDays'] for m in products
                        if m['latenessDays'] < 999999), default=0)
    total_lateness = sum(max(0, m['latenessDays']) for m in products
                         if m['latenessDays'] < 999999)

    # Get optimal capacities for scenario 2 (if it exists)
    optimal_mechanics = result.get('optimal_mechanics') if result else None
    optimal_quality = result.get('optimal_quality') if result else None

    # For scenario 3, get the achieved minimum lateness
    if scenario_name == 'scenario3' and result:
        achieved_max_lateness = result.get('max_lateness', max_lateness)
    else:
        achieved_max_lateness = max_lateness

    # Calculate total unique tasks across all products
    all_unique_tasks = set()
    for task_id in scheduler.tasks:
        _, task_num = scheduler.parse_product_task_id(task_id)
        if task_num:
            all_unique_tasks.add(task_num)

    # For scenario 2, add target lateness and deviation info
    target_lateness = None
    max_deviation = None
    optimal_found = False
    if scenario_name == 'scenario2' and result:
        target_lateness = result.get('target_lateness', -1)
        max_deviation = result.get('max_deviation', 0)
        optimal_found = result.get('total_workforce') is not None

    # Include baseline constraint information for debugging
    baseline_constraint_count = len([c for c in scheduler.precedence_constraints])
    tasks_with_deps = sum(1 for t in tasks if t['dependencies'])
    tasks_with_succs = sum(1 for t in tasks if t['successors'])

    print(f"[DEBUG] Exporting scenario {scenario_name}:")
    print(f"  - Total tasks: {len(tasks)}")
    print(f"  - Tasks with dependencies: {tasks_with_deps}")
    print(f"  - Tasks with successors: {tasks_with_succs}")
    print(f"  - Critical tasks across all products: {sum(p['criticalTasks'] for p in products)}")
    print(f"  - Baseline constraints in scheduler: {baseline_constraint_count}")

    # Return complete scenario data
    return {
        'scenarioName': scenario_name,
        'totalWorkforce': total_workforce,
        'makespan': makespan,
        'onTimeRate': on_time_rate,
        'avgUtilization': avg_utilization,
        'maxLateness': max_lateness,
        'totalLateness': total_lateness,
        'achievedMaxLateness': achieved_max_lateness,
        'teamCapacities': team_capacities,
        'tasks': tasks,
        'products': products,
        'utilization': utilization_data,
        'totalTasks': len(scheduler.tasks),
        'totalUniqueTaskNums': len(all_unique_tasks),
        'scheduledTasks': len(scheduler.task_schedule),
        'taskTypeSummary': dict(task_type_summary),
        'optimalMechanics': optimal_mechanics,
        'optimalQuality': optimal_quality,
        'optimalFound': optimal_found,
        'targetLateness': target_lateness,
        'maxDeviation': max_deviation,
        'teams': list(team_capacities.keys()),
        'holidays': []  # Will be populated by separate endpoint if needed
    }


def create_failed_scenario_data():
    """Create placeholder data for failed scenarios"""
    return {
        'scenarioName': 'scenario3',
        'totalWorkforce': 0,
        'makespan': 999999,
        'onTimeRate': 0,
        'avgUtilization': 0,
        'maxLateness': 999999,
        'totalLateness': 999999,
        'teamCapacities': {},
        'tasks': [],
        'products': [],
        'utilization': {},
        'totalTasks': 0,
        'scheduledTasks': 0,
        'error': 'Failed to find solution within constraints'
    }


def calculate_team_utilization(scheduler, team_capacities=None, quality_capacities=None):
    """Calculate utilization percentage for each team"""
    utilization = {}

    if not scheduler.task_schedule:
        return utilization

    # Use provided capacities or fall back to scheduler's current ones
    mech_capacities = team_capacities if team_capacities is not None else scheduler.team_capacity
    qual_capacities = quality_capacities if quality_capacities is not None else scheduler.quality_team_capacity

    # Working minutes per shift
    minutes_per_shift = 8.5 * 60
    makespan_days = scheduler.calculate_makespan()

    if makespan_days == 0 or makespan_days >= 999999:
        return utilization

    # Calculate for mechanic teams
    for team, capacity in mech_capacities.items():
        scheduled_minutes = 0
        task_count = 0

        for task_id, schedule in scheduler.task_schedule.items():
            if schedule['team'] == team:
                scheduled_minutes += schedule['duration'] * schedule['mechanics_required']
                task_count += 1

        shifts_per_day = len(scheduler.team_shifts.get(team, []))
        available_minutes = capacity * shifts_per_day * minutes_per_shift * makespan_days

        if available_minutes > 0:
            util_percent = min(100, int((scheduled_minutes / available_minutes) * 100))
            utilization[team] = util_percent
        else:
            utilization[team] = 0

    # Calculate for quality teams
    for team, capacity in qual_capacities.items():
        scheduled_minutes = 0
        task_count = 0

        for task_id, schedule in scheduler.task_schedule.items():
            if schedule['team'] == team:
                scheduled_minutes += schedule['duration'] * schedule['mechanics_required']
                task_count += 1

        shifts_per_day = len(scheduler.quality_team_shifts.get(team, []))
        available_minutes = capacity * shifts_per_day * minutes_per_shift * makespan_days

        if available_minutes > 0:
            util_percent = min(100, int((scheduled_minutes / available_minutes) * 100))
            utilization[team] = util_percent
        else:
            utilization[team] = 0

    return utilization


# Flask Routes

@app.route('/')
def index():
    """Serve the main dashboard page"""
    # Trigger lazy initialization if not started
    if not initialization_status['initialized'] and not initialization_status['initializing']:
        initialize_scheduler_lazy()
    return render_template('dashboard.html')


@app.route('/api/initialization_status')
def get_initialization_status():
    """Get the current initialization status"""
    status = {
        'initialized': initialization_status['initialized'],
        'initializing': initialization_status['initializing'],
        'progress': initialization_status['progress'],
        'currentScenario': initialization_status['current_scenario'],
        'scenariosCompleted': initialization_status['scenarios_completed'],
        'error': initialization_status['error']
    }

    if initialization_status['start_time']:
        elapsed = (datetime.now() - initialization_status['start_time']).total_seconds()
        status['elapsedSeconds'] = int(elapsed)

    if initialization_status['end_time']:
        total_time = (initialization_status['end_time'] - initialization_status['start_time']).total_seconds()
        status['totalSeconds'] = int(total_time)

    return jsonify(status)


@app.route('/api/scenarios')
def get_scenarios():
    """Get list of available scenarios with descriptions"""
    # Check if we need to start initialization
    if not initialization_status['initialized'] and not initialization_status['initializing']:
        initialize_scheduler_lazy()

    scenarios = []

    # Always show scenario descriptions
    scenario_descriptions = [
        {
            'id': 'baseline',
            'name': 'Baseline (CSV Capacity)',
            'description': 'Original capacity from CSV file',
            'available': 'baseline' in scenario_results
        },
        {
            'id': 'scenario1',
            'name': 'Scenario 1: CSV Headcount',
            'description': 'Schedule with CSV-defined headcount, allow late delivery',
            'available': 'scenario1' in scenario_results
        },
        {
            'id': 'scenario2',
            'name': 'Scenario 2: Just-In-Time',
            'description': 'Optimize for target delivery timing',
            'available': 'scenario2' in scenario_results
        },
        {
            'id': 'scenario3',
            'name': 'Scenario 3: Multi-Dimensional',
            'description': 'Optimize per-team capacity for minimum lateness',
            'available': 'scenario3' in scenario_results
        }
    ]

    return jsonify({
        'scenarios': scenario_descriptions,
        'loading': initialization_status['initializing'],
        'error': initialization_status['error']
    })


@app.route('/api/scenario/<scenario_id>')
def get_scenario_data(scenario_id):
    """Get data for a specific scenario"""
    # Check if still loading
    if initialization_status['initializing']:
        return jsonify({
            'loading': True,
            'progress': initialization_status['progress'],
            'currentScenario': initialization_status['current_scenario']
        }), 202  # Accepted but not complete

    if scenario_id in scenario_results:
        return jsonify(scenario_results[scenario_id])
    else:
        return jsonify({'error': 'Scenario not found or still loading'}), 404


@app.route('/api/scenario/<scenario_id>/summary')
def get_scenario_summary(scenario_id):
    """Get summary statistics for a scenario"""
    if initialization_status['initializing']:
        return jsonify({
            'loading': True,
            'progress': initialization_status['progress']
        }), 202

    if scenario_id not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404

    data = scenario_results[scenario_id]

    # Calculate product-specific summaries
    product_summaries = []
    for product in data.get('products', []):
        product_summaries.append({
            'name': product['name'],
            'status': 'On Time' if product['onTime'] else f"Late by {product['latenessDays']} days",
            'latePartsCount': product.get('latePartsCount', 0),
            'reworkCount': product.get('reworkCount', 0),
            'totalTasks': product['totalTasks'],
            'uniqueTasks': product.get('uniqueTasks', 0)
        })

    summary = {
        'scenarioName': data['scenarioName'],
        'totalWorkforce': data['totalWorkforce'],
        'makespan': data['makespan'],
        'onTimeRate': data['onTimeRate'],
        'avgUtilization': data['avgUtilization'],
        'maxLateness': data.get('maxLateness', 0),
        'totalLateness': data.get('totalLateness', 0),
        'achievedMaxLateness': data.get('achievedMaxLateness', data.get('maxLateness', 0)),
        'totalTasks': data['totalTasks'],
        'totalUniqueTaskNums': data.get('totalUniqueTaskNums', 0),
        'scheduledTasks': data['scheduledTasks'],
        'taskTypeSummary': data.get('taskTypeSummary', {}),
        'productSummaries': product_summaries
    }

    return jsonify(summary)


@app.route('/api/team/<team_name>/tasks')
def get_team_tasks(team_name):
    """Get tasks for a specific team with product-task instance info"""
    scenario = request.args.get('scenario', 'baseline')
    shift = request.args.get('shift', 'all')
    limit = int(request.args.get('limit', 50))
    start_date = request.args.get('date', None)

    if scenario not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404

    tasks = scenario_results[scenario]['tasks']

    # Filter by team
    if team_name != 'all':
        tasks = [t for t in tasks if t['team'] == team_name]

    # Filter by shift
    if shift != 'all':
        tasks = [t for t in tasks if t['shift'] == shift]

    # Filter by date if provided
    if start_date:
        target_date = datetime.fromisoformat(start_date).date()
        tasks = [t for t in tasks
                 if datetime.fromisoformat(t['startTime']).date() == target_date]

    # Sort by start time and limit
    tasks.sort(key=lambda x: x['startTime'])
    tasks = tasks[:limit]

    # Add team capacity info
    team_capacity = scenario_results[scenario]['teamCapacities'].get(team_name, 0)
    team_shifts = []
    if scheduler and team_name in scheduler.team_shifts:
        team_shifts = scheduler.team_shifts[team_name]
    elif scheduler and team_name in scheduler.quality_team_shifts:
        team_shifts = scheduler.quality_team_shifts[team_name]

    return jsonify({
        'tasks': tasks,
        'total': len(tasks),
        'teamCapacity': team_capacity,
        'teamShifts': team_shifts,
        'utilization': scenario_results[scenario]['utilization'].get(team_name, 0)
    })


@app.route('/api/product/<product_name>/tasks')
def get_product_tasks(product_name):
    """Get all tasks for a specific product including late parts and rework"""
    scenario = request.args.get('scenario', 'baseline')

    if scenario not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404

    tasks = scenario_results[scenario]['tasks']

    # Filter by product
    product_tasks = [t for t in tasks if t['product'] == product_name]

    # Separate by task type
    task_breakdown = defaultdict(list)
    unique_task_nums = set()
    for task in product_tasks:
        task_breakdown[task['type']].append(task)
        if task.get('taskNum'):
            unique_task_nums.add(task['taskNum'])

    # Sort each type by start time
    for task_type in task_breakdown:
        task_breakdown[task_type].sort(key=lambda x: x['startTime'])

    # Get product info
    product_info = next((p for p in scenario_results[scenario]['products']
                         if p['name'] == product_name), None)

    return jsonify({
        'productName': product_name,
        'productInfo': product_info,
        'tasks': product_tasks,
        'taskBreakdown': {k: len(v) for k, v in task_breakdown.items()},
        'tasksByType': dict(task_breakdown),
        'totalTasks': len(product_tasks),
        'uniqueTaskNums': len(unique_task_nums)
    })


@app.route('/api/team/<team_name>/generate_assignments', methods=['POST'])
def generate_team_assignments(team_name):
    """Generate individual mechanic assignments based on actual attendance"""
    data = request.json
    scenario = data.get('scenario', 'baseline')
    date = data.get('date', datetime.now().isoformat())
    present_mechanics = data.get('presentMechanics', [])  # List of mechanic names who showed up
    is_overtime_day = data.get('isOvertimeDay', False)  # Flag for weekend/overtime work

    if scenario not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404

    # Get all tasks for this team
    tasks = scenario_results[scenario]['tasks']
    target_date = datetime.fromisoformat(date).date()

    # Check if it's a weekend
    is_weekend = target_date.weekday() in [5, 6]  # Saturday = 5, Sunday = 6

    # If it's a weekend or overtime day, calculate workable tasks
    if is_weekend or is_overtime_day:
        # Find tasks that can actually be worked on this overtime day
        team_tasks = get_workable_tasks_for_team(
            team_name=team_name,
            overtime_date=target_date,
            scenario=scenario,
            present_mechanics=present_mechanics
        )
    else:
        # Regular day - get normally scheduled tasks
        team_tasks = [t for t in tasks
                      if t.get('team') == team_name
                      and datetime.fromisoformat(t['startTime']).date() == target_date]

    # Sort by priority (critical first, then by start time)
    team_tasks.sort(key=lambda t: (
        not t.get('isCritical', False),  # Critical tasks first
        not t.get('isLatePartTask', False),  # Late parts second
        t.get('priority', 999999),  # Then by priority score
        t.get('startTime')  # Then by start time
    ))

    # Rest of the function remains the same...
    # Configuration
    SHIFT_DURATION_MINUTES = 8.5 * 60  # 510 minutes
    BREAK_DURATION_MINUTES = 50  # 30 min lunch + 20 min breaks
    WORKING_MINUTES_PER_SHIFT = SHIFT_DURATION_MINUTES - BREAK_DURATION_MINUTES  # 460 minutes
    MAX_OVERTIME_MINUTES = SHIFT_DURATION_MINUTES * 0.2  # 102 minutes (20% overtime)
    MAX_WORKING_MINUTES = WORKING_MINUTES_PER_SHIFT + MAX_OVERTIME_MINUTES  # 562 minutes

    # Initialize mechanic schedules
    mechanic_schedules = {name: {
        'tasks': [],
        'totalMinutes': 0,
        'overtimeMinutes': 0,
        'utilizationPercent': 0
    } for name in present_mechanics}

    # Track when each mechanic will be free
    mechanic_free_at = {name: datetime.fromisoformat(date).replace(hour=6, minute=0)
                        for name in present_mechanics}

    # Assign tasks
    unassigned_tasks = []

    for task in team_tasks:
        task_start = datetime.fromisoformat(task['startTime'])
        task_end = datetime.fromisoformat(task['endTime'])
        task_duration = task['duration']
        mechanics_needed = task.get('mechanics', 1)

        # Find mechanics available at task start time
        available_mechanics = []
        for mechanic_name in present_mechanics:
            # Check if mechanic is free
            if mechanic_free_at[mechanic_name] <= task_start:
                # Check if they have capacity left (including overtime)
                minutes_left = MAX_WORKING_MINUTES - mechanic_schedules[mechanic_name]['totalMinutes']
                if minutes_left >= task_duration:
                    available_mechanics.append(mechanic_name)

        # Assign if enough mechanics available
        if len(available_mechanics) >= mechanics_needed:
            # Sort by least utilized first (load balancing)
            available_mechanics.sort(key=lambda m: mechanic_schedules[m]['totalMinutes'])
            assigned_mechanics = available_mechanics[:mechanics_needed]

            # Create task assignment for each mechanic
            for mechanic_name in assigned_mechanics:
                task_assignment = {
                    'taskId': task['taskId'],
                    'displayName': task.get('displayName', task['taskId']),
                    'type': task['type'],
                    'product': task['product'],
                    'startTime': task['startTime'],
                    'endTime': task['endTime'],
                    'duration': task_duration,
                    'isCritical': task.get('isCritical', False),
                    'isLatePartTask': task.get('isLatePartTask', False),
                    'assignedWith': [m for m in assigned_mechanics if m != mechanic_name]
                }

                mechanic_schedules[mechanic_name]['tasks'].append(task_assignment)
                mechanic_schedules[mechanic_name]['totalMinutes'] += task_duration
                mechanic_free_at[mechanic_name] = task_end

                # Calculate overtime if applicable
                if mechanic_schedules[mechanic_name]['totalMinutes'] > WORKING_MINUTES_PER_SHIFT:
                    overtime = mechanic_schedules[mechanic_name]['totalMinutes'] - WORKING_MINUTES_PER_SHIFT
                    mechanic_schedules[mechanic_name]['overtimeMinutes'] = min(overtime, MAX_OVERTIME_MINUTES)
        else:
            # Can't assign - not enough mechanics
            unassigned_tasks.append({
                **task,
                'reason': f'Need {mechanics_needed} mechanics, only {len(available_mechanics)} available',
                'availableMechanics': available_mechanics
            })

    # Calculate utilization and overtime for each mechanic
    for mechanic_name in present_mechanics:
        total_minutes = mechanic_schedules[mechanic_name]['totalMinutes']
        mechanic_schedules[mechanic_name]['utilizationPercent'] = round(
            (total_minutes / WORKING_MINUTES_PER_SHIFT) * 100, 1
        )

        # Sort tasks by start time for display
        mechanic_schedules[mechanic_name]['tasks'].sort(key=lambda t: t['startTime'])

    # Calculate team statistics
    team_stats = {
        'requiredCapacity': scenario_results[scenario]['teamCapacities'].get(team_name, 0),
        'actualCapacity': len(present_mechanics),
        'totalTasks': len(team_tasks),
        'assignedTasks': len(team_tasks) - len(unassigned_tasks),
        'unassignedTasks': len(unassigned_tasks),
        'criticalUnassigned': sum(1 for t in unassigned_tasks if t.get('isCritical')),
        'teamUtilization': round(
            sum(m['totalMinutes'] for m in mechanic_schedules.values()) /
            (len(present_mechanics) * WORKING_MINUTES_PER_SHIFT) * 100, 1
        ) if present_mechanics else 0,
        'totalOvertimeMinutes': sum(m['overtimeMinutes'] for m in mechanic_schedules.values()),
        'mechanicsRequiringOvertime': sum(1 for m in mechanic_schedules.values() if m['overtimeMinutes'] > 0)
    }

    # Add overtime-specific information if it's an overtime day
    if is_weekend or is_overtime_day:
        team_stats['isOvertimeDay'] = True
        team_stats['pulledFromDate'] = 'Next working day (Monday)'

    return jsonify({
        'team': team_name,
        'date': date,
        'scenario': scenario,
        'teamStats': team_stats,
        'mechanicAssignments': mechanic_schedules,
        'unassignedTasks': unassigned_tasks,
        'warnings': generate_assignment_warnings(unassigned_tasks, team_stats)
    })


def get_workable_tasks_for_team(team_name, overtime_date, scenario, present_mechanics):
    """
    Get tasks that can be worked on an overtime day for a specific team
    This integrates with the existing overtime logic to find pullable tasks
    """
    if scenario not in scenario_results:
        return []

    tasks = scenario_results[scenario]['tasks']

    # Step 1: Identify all tasks completed by end of Friday (or previous working day)
    completed_tasks = set()
    for task in tasks:
        task_end = datetime.fromisoformat(task['endTime'])
        if task_end.date() < overtime_date:
            completed_tasks.add(task['taskId'])

    # Step 2: Find next working day's tasks for this team
    next_monday = overtime_date
    while next_monday.weekday() >= 5:
        next_monday += timedelta(days=1)

    monday_tasks = [t for t in tasks
                    if datetime.fromisoformat(t['startTime']).date() == next_monday
                    and t['team'] == team_name]

    # Step 3: Identify workable tasks (those with all dependencies satisfied)
    workable_tasks = []

    for task in monday_tasks:
        # Check if ALL dependencies are satisfied
        can_work = True

        if task.get('dependencies'):
            for dep in task['dependencies']:
                dep_id = dep.get('taskId') if isinstance(dep, dict) else dep

                if dep_id not in completed_tasks:
                    # Check if dependency is also from the same team and could be done Saturday
                    dep_task = next((t for t in tasks if t['taskId'] == dep_id), None)

                    if dep_task:
                        # If dependency is from a different team that's not working, can't do this task
                        if dep_task['team'] != team_name:
                            can_work = False
                            break
                        # If dependency is from same team but has its own unsatisfied dependencies, can't do
                        elif dep_task.get('dependencies'):
                            # Would need recursive check here, but for simplicity, skip
                            can_work = False
                            break

        if can_work:
            workable_tasks.append(task)

    # Step 4: Sort workable tasks by priority
    workable_tasks.sort(key=lambda t: (
        not t.get('isCritical', False),
        not t.get('isLatePartTask', False),
        t.get('priority', 999999)
    ))

    return workable_tasks


@app.route('/api/overtime/workable_tasks', methods=['POST'])
def get_workable_tasks_for_overtime():
    """
    Calculate which tasks can actually be worked on an overtime day
    considering cross-team dependencies and partial crew availability
    """
    data = request.json
    scenario = data.get('scenario', 'baseline')
    date = data.get('date', datetime.now().isoformat())
    working_teams = data.get('workingTeams', {})  # { 'Mechanic Team 1': ['Mech1', 'Mech2'], ... }

    if scenario not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404

    tasks = scenario_results[scenario]['tasks']
    overtime_date = datetime.fromisoformat(date).date()

    # Step 1: Identify all tasks completed by end of Friday (or previous working day)
    completed_tasks = set()
    for task in tasks:
        task_end = datetime.fromisoformat(task['endTime'])
        if task_end.date() < overtime_date:
            completed_tasks.add(task['taskId'])

    # Step 2: Find next working day's tasks
    next_monday = overtime_date
    while next_monday.weekday() >= 5:
        next_monday += timedelta(days=1)

    monday_tasks = [t for t in tasks
                    if datetime.fromisoformat(t['startTime']).date() == next_monday]

    # Step 3: Identify workable tasks
    workable_tasks = []

    for task in monday_tasks:
        # Check if task's team is working
        if task['team'] not in working_teams:
            continue

        # Check if team has enough people for this task
        available_mechanics = len(working_teams[task['team']])
        if available_mechanics < task.get('mechanics', 1):
            continue

        # Check if ALL dependencies are satisfied
        can_work = True
        dependency_status = []

        if task.get('dependencies'):
            for dep in task['dependencies']:
                dep_id = dep.get('taskId') if isinstance(dep, dict) else dep

                if dep_id not in completed_tasks:
                    # Check if dependency is also workable on Saturday
                    dep_task = next((t for t in tasks if t['taskId'] == dep_id), None)

                    if dep_task:
                        # Is the dependency's team working?
                        if dep_task['team'] not in working_teams:
                            can_work = False
                            dependency_status.append({
                                'taskId': dep_id,
                                'team': dep_task['team'],
                                'status': 'team_not_working'
                            })
                        else:
                            # Dependency could potentially be done Saturday too
                            dependency_status.append({
                                'taskId': dep_id,
                                'team': dep_task['team'],
                                'status': 'needs_concurrent_completion'
                            })

        if can_work:
            workable_tasks.append({
                **task,
                'dependencyStatus': dependency_status,
                'requiredMechanics': task.get('mechanics', 1),
                'availableMechanics': available_mechanics
            })

    # Step 4: Sort workable tasks by priority and dependencies
    # Tasks with no Saturday dependencies should go first
    workable_tasks.sort(key=lambda t: (
        len([d for d in t['dependencyStatus'] if d['status'] == 'needs_concurrent_completion']),
        not t.get('isCritical', False),
        not t.get('isLatePartTask', False),
        t.get('priority', 999999)
    ))

    # Step 5: Build execution order respecting dependencies
    scheduled_saturday_tasks = []
    completed_saturday = set(completed_tasks)  # Include Friday's completed work

    max_iterations = len(workable_tasks) * 2
    iterations = 0

    while workable_tasks and iterations < max_iterations:
        iterations += 1
        tasks_scheduled_this_pass = []

        for task in workable_tasks[:]:
            # Check if all dependencies are now satisfied
            all_deps_satisfied = True

            if task.get('dependencies'):
                for dep in task['dependencies']:
                    dep_id = dep.get('taskId') if isinstance(dep, dict) else dep
                    if dep_id not in completed_saturday:
                        all_deps_satisfied = False
                        break

            if all_deps_satisfied:
                scheduled_saturday_tasks.append(task)
                completed_saturday.add(task['taskId'])
                workable_tasks.remove(task)
                tasks_scheduled_this_pass.append(task)

        # If no tasks were scheduled this pass, we have unresolvable dependencies
        if not tasks_scheduled_this_pass:
            break

    # Step 6: Group by team and calculate workload
    team_workloads = {}
    for team, mechanics in working_teams.items():
        team_tasks = [t for t in scheduled_saturday_tasks if t['team'] == team]
        total_minutes = sum(t.get('duration', 60) * t.get('mechanics', 1) for t in team_tasks)
        capacity_minutes = len(mechanics) * 8 * 60  # 8 hours per mechanic

        team_workloads[team] = {
            'tasks': team_tasks,
            'totalMinutes': total_minutes,
            'capacityMinutes': capacity_minutes,
            'utilization': (total_minutes / capacity_minutes * 100) if capacity_minutes > 0 else 0,
            'canComplete': total_minutes <= capacity_minutes
        }

    # Find unworkable tasks due to dependencies
    unworkable_tasks = []
    for task in monday_tasks:
        if task['taskId'] not in [t['taskId'] for t in scheduled_saturday_tasks]:
            if task['team'] in working_teams:
                # Team is working but task can't be done
                reason = 'Dependencies not met'
                if task.get('dependencies'):
                    missing_deps = []
                    for dep in task['dependencies']:
                        dep_id = dep.get('taskId') if isinstance(dep, dict) else dep
                        if dep_id not in completed_saturday:
                            dep_task = next((t for t in tasks if t['taskId'] == dep_id), None)
                            if dep_task:
                                missing_deps.append({
                                    'taskId': dep_id,
                                    'team': dep_task['team'],
                                    'working': dep_task['team'] in working_teams
                                })
                    reason = f"Missing dependencies: {missing_deps}"

                unworkable_tasks.append({
                    'taskId': task['taskId'],
                    'team': task['team'],
                    'reason': reason
                })

    return jsonify({
        'overtimeDate': overtime_date.isoformat(),
        'workingTeams': list(working_teams.keys()),
        'totalWorkableTasks': len(scheduled_saturday_tasks),
        'scheduledTasks': scheduled_saturday_tasks,
        'teamWorkloads': team_workloads,
        'unworkableTasks': unworkable_tasks,
        'warnings': generate_overtime_warnings(team_workloads, unworkable_tasks)
    })


def generate_overtime_warnings(team_workloads, unworkable_tasks):
    warnings = []

    for team, workload in team_workloads.items():
        if workload['utilization'] > 100:
            warnings.append({
                'level': 'critical',
                'message': f"{team} is overloaded: {workload['utilization']:.1f}% utilization"
            })
        elif workload['utilization'] > 85:
            warnings.append({
                'level': 'warning',
                'message': f"{team} is near capacity: {workload['utilization']:.1f}% utilization"
            })

    if len(unworkable_tasks) > 0:
        critical_unworkable = [t for t in unworkable_tasks if 'critical' in str(t).lower()]
        if critical_unworkable:
            warnings.append({
                'level': 'critical',
                'message': f"{len(critical_unworkable)} critical tasks cannot be worked due to dependencies"
            })

    return warnings

def generate_assignment_warnings(unassigned_tasks, team_stats):
    """Generate warnings for the team lead"""
    warnings = []

    if team_stats['criticalUnassigned'] > 0:
        warnings.append({
            'level': 'critical',
            'message': f"{team_stats['criticalUnassigned']} critical tasks cannot be assigned due to insufficient staff"
        })

    if team_stats['totalOvertimeMinutes'] > 0:
        overtime_hours = round(team_stats['totalOvertimeMinutes'] / 60, 1)
        warnings.append({
            'level': 'warning',
            'message': f"{team_stats['mechanicsRequiringOvertime']} mechanics need {overtime_hours} total overtime hours"
        })

    if team_stats['actualCapacity'] < team_stats['requiredCapacity']:
        shortage = team_stats['requiredCapacity'] - team_stats['actualCapacity']
        warnings.append({
            'level': 'warning',
            'message': f"Team is short {shortage} mechanics from required capacity"
        })

    return warnings

@app.route('/api/mechanic/<mechanic_id>/tasks')
def get_mechanic_tasks(mechanic_id):
    """Get tasks assigned to a specific mechanic"""
    scenario = request.args.get('scenario', 'baseline')
    date = request.args.get('date', datetime.now().isoformat())

    if scenario not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404

    # For demo purposes, assign tasks based on mechanic ID pattern
    tasks = scenario_results[scenario]['tasks']

    # Simple assignment logic for demo
    mechanic_num = int(''.join(filter(str.isdigit, mechanic_id))) if any(c.isdigit() for c in mechanic_id) else 1
    assigned_tasks = []

    # Filter tasks by date
    target_date = datetime.fromisoformat(date).date()
    daily_tasks = [t for t in tasks if datetime.fromisoformat(t['startTime']).date() == target_date]

    # Assign every Nth task to this mechanic (simple demo logic)
    for i, task in enumerate(daily_tasks):
        if i % 8 == (mechanic_num - 1):  # Distribute among 8 mechanics
            assigned_tasks.append(task)
            if len(assigned_tasks) >= 6:  # Max 6 tasks per day
                break

    # Sort by start time
    assigned_tasks.sort(key=lambda x: x['startTime'])

    return jsonify({
        'mechanicId': mechanic_id,
        'tasks': assigned_tasks,
        'shift': '1st',  # Would be determined by actual assignment
        'date': date,
        'totalAssigned': len(assigned_tasks)
    })


@app.route('/api/simulate_priority', methods=['POST'])
def simulate_priority():
    """Simulate the impact of prioritizing a specific product"""
    data = request.json
    product = data.get('product')
    level = data.get('level', 'high')
    days = data.get('days', 30)
    scenario = data.get('scenario', 'baseline')

    # Resource multipliers for different priority levels
    resource_multipliers = {
        'high': 1.5,
        'critical': 1.75,
        'exclusive': 2.0
    }

    multiplier = resource_multipliers.get(level, 1.5)

    # In production, this would run actual scheduling simulation
    # For now, provide estimated impacts

    other_products = ['Product A', 'Product B', 'Product C', 'Product D', 'Product E']
    other_products.remove(product) if product in other_products else None

    # Simulate impact
    results = {
        'prioritizedProduct': {
            'name': product,
            'originalLateness': 3,
            'newLateness': 0,
            'completionImprovement': int(20 * (multiplier - 1) + 15),
            'resourceAllocation': int(multiplier * 20)
        },
        'impactedProducts': []
    }

    # Calculate delays for other products
    for other_product in other_products:
        base_lateness = 2
        additional_delay = int((multiplier - 1) * 3)

        results['impactedProducts'].append({
            'name': other_product,
            'originalLateness': base_lateness,
            'newLateness': base_lateness + additional_delay,
            'delayAdded': additional_delay,
            'resourceReduction': int((multiplier - 1) * 15)
        })

    return jsonify(results)


@app.route('/api/late_parts_impact/<scenario_id>')
def get_late_parts_impact(scenario_id):
    """Calculate the ACTUAL impact of late parts for this specific scenario's schedule"""
    if scenario_id not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404

    data = scenario_results[scenario_id]
    tasks = data.get('tasks', [])
    products = data.get('products', [])

    # Get the scenario-specific configuration
    scenario_config = {
        'workforce': data.get('totalWorkforce', 0),
        'makespan': data.get('makespan', 0),
        'team_capacities': data.get('teamCapacities', {}),
        'scenario_name': data.get('scenarioName', scenario_id)
    }

    product_impacts = {}
    total_schedule_pushout = 0.0
    critical_path_disruptions = 0

    for product in products:
        product_name = product['name']
        product_tasks = [t for t in tasks if t.get('product') == product_name]

        # Get late parts and rework for this product
        late_part_tasks = [t for t in product_tasks if t.get('isLatePartTask', False)]
        rework_tasks = [t for t in product_tasks if t.get('isReworkTask', False)]

        if not late_part_tasks and not rework_tasks:
            product_impacts[product_name] = {
                'latePartCount': 0,
                'reworkCount': 0,
                'scenarioImpact': 0.0,
                'criticalPathImpact': False,
                'schedulePushout': 0.0,
                'productLateness': product.get('latenessDays', 0),
                'onTime': product.get('onTime', True)
            }
            continue

        # Calculate SCENARIO-SPECIFIC impact
        scenario_impact = 0.0
        schedule_pushout = 0.0
        critical_impact = False

        for lp_task in late_part_tasks:
            # Get this task's specific schedule in THIS scenario
            task_start = pd.to_datetime(lp_task.get('startTime'))
            task_end = pd.to_datetime(lp_task.get('endTime'))
            slack_hours = lp_task.get('slackHours')

            # Calculate how this late part affects THIS scenario's schedule
            # The impact depends on:
            # 1. When it's scheduled (earlier = more downstream impact)
            # 2. How much slack it has (less slack = more critical)
            # 3. How many tasks depend on it

            # Find dependent tasks
            dependent_count = 0
            total_dependent_duration = 0

            for other_task in product_tasks:
                if other_task.get('dependencies'):
                    for dep in other_task['dependencies']:
                        dep_id = dep.get('taskId') if isinstance(dep, dict) else dep
                        if dep_id == lp_task.get('taskId'):
                            dependent_count += 1
                            total_dependent_duration += other_task.get('duration', 0)

                            # If dependent task has low slack, this is critical
                            if other_task.get('slackHours', float('inf')) < 24:
                                critical_impact = True

            # Calculate schedule pushout based on scenario
            if slack_hours is not None and slack_hours < 24:
                # Critical path task - directly impacts makespan
                task_duration_days = lp_task.get('duration', 0) / (60 * 8)

                # In scenarios with less capacity, impact is amplified
                capacity_factor = 1.0
                if scenario_config['workforce'] > 0:
                    baseline_workforce = scenario_results.get('baseline', {}).get('totalWorkforce', 100)
                    capacity_factor = baseline_workforce / scenario_config['workforce']

                schedule_pushout += task_duration_days * capacity_factor
                scenario_impact += task_duration_days * capacity_factor * (1 + dependent_count * 0.1)
            else:
                # Non-critical but still causes local delays
                task_duration_days = lp_task.get('duration', 0) / (60 * 8)
                scenario_impact += task_duration_days * 0.5

        # Add rework impact (scenario-specific)
        for rw_task in rework_tasks:
            duration_days = rw_task.get('duration', 0) / (60 * 8)
            slack = rw_task.get('slackHours', float('inf'))

            if slack < 24:
                # Rework on critical path
                schedule_pushout += duration_days
                scenario_impact += duration_days
                critical_impact = True
            else:
                scenario_impact += duration_days * 0.3

        if critical_impact:
            critical_path_disruptions += 1

        total_schedule_pushout += schedule_pushout

        product_impacts[product_name] = {
            'latePartCount': len(late_part_tasks),
            'reworkCount': len(rework_tasks),
            'scenarioImpact': round(scenario_impact, 2),
            'schedulePushout': round(schedule_pushout, 2),
            'criticalPathImpact': critical_impact,
            'productLateness': product.get('latenessDays', 0),
            'onTime': product.get('onTime', False)
        }

    # Calculate scenario-specific metrics
    total_late_parts = sum(p['latePartCount'] for p in product_impacts.values())
    total_rework = sum(p['reworkCount'] for p in product_impacts.values())
    total_impact = sum(p['scenarioImpact'] for p in product_impacts.values())

    # Compare to baseline if available
    impact_vs_baseline = None
    if 'baseline' in scenario_results and scenario_id != 'baseline':
        baseline_response = get_late_parts_impact('baseline')
        if baseline_response.status_code == 200:
            baseline_data = baseline_response.get_json()
            baseline_impact = baseline_data.get('overallStatistics', {}).get('totalScenarioImpact', 0)
            if baseline_impact > 0:
                impact_vs_baseline = ((total_impact - baseline_impact) / baseline_impact) * 100

    return jsonify({
        'scenarioInfo': scenario_config,
        'overallStatistics': {
            'totalLatePartsCount': total_late_parts,
            'totalReworkCount': total_rework,
            'totalScenarioImpact': round(total_impact, 2),
            'totalSchedulePushout': round(total_schedule_pushout, 2),
            'criticalPathDisruptions': critical_path_disruptions,
            'productsWithLateParts': sum(1 for p in product_impacts.values() if p['latePartCount'] > 0),
            'totalProducts': len(products),
            'impactVsBaseline': round(impact_vs_baseline, 1) if impact_vs_baseline else None
        },
        'productImpacts': product_impacts,
        'scenarioAnalysis': {
            'workforce': scenario_config['workforce'],
            'makespan': scenario_config['makespan'],
            'avgImpactPerLatePart': round(total_impact / total_late_parts, 2) if total_late_parts > 0 else 0,
            'criticalPathSensitivity': 'High' if critical_path_disruptions > 2 else 'Medium' if critical_path_disruptions > 0 else 'Low'
        }
    })


@app.route('/api/bottleneck_analysis/<scenario_id>')
def analyze_bottlenecks(scenario_id):
    """Identify bottlenecks in the schedule"""
    if scenario_id not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404

    data = scenario_results[scenario_id]

    # Analyze team utilization for bottlenecks
    bottlenecks = []

    for team, utilization in data.get('utilization', {}).items():
        if utilization > 90:
            bottlenecks.append({
                'team': team,
                'utilization': utilization,
                'severity': 'critical' if utilization > 95 else 'high'
            })
        elif utilization > 80:
            bottlenecks.append({
                'team': team,
                'utilization': utilization,
                'severity': 'medium'
            })

    # Sort by utilization
    bottlenecks.sort(key=lambda x: x['utilization'], reverse=True)

    return jsonify({'bottlenecks': bottlenecks})


@app.route('/api/export/<scenario_id>')
def export_scenario(scenario_id):
    """Export scenario data to CSV"""
    if scenario_id not in scenario_results:
        return jsonify({'error': 'Scenario not found'}), 404

    data = scenario_results[scenario_id]

    # Create DataFrame from tasks
    df = pd.DataFrame(data['tasks'])

    # Add additional columns
    df['Scenario'] = scenario_id
    df['MaxLateness'] = data.get('maxLateness', 0)
    df['TotalLateness'] = data.get('totalLateness', 0)

    # Save to CSV
    filename = f'export_{scenario_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    filepath = os.path.join('exports', filename)

    # Create exports directory if it doesn't exist
    os.makedirs('exports', exist_ok=True)

    df.to_csv(filepath, index=False)

    return send_file(filepath, as_attachment=True, download_name=filename)


@app.route('/api/assign_task', methods=['POST'])
def assign_task():
    """Assign a task to a mechanic"""
    data = request.json
    task_id = data.get('taskId')
    mechanic_id = data.get('mechanicId')
    scenario = data.get('scenario', 'baseline')

    # In production, this would update a database
    # For now, just return success
    return jsonify({
        'success': True,
        'taskId': task_id,
        'mechanicId': mechanic_id,
        'message': f'Task {task_id} assigned to {mechanic_id}'
    })


@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    """Refresh all scenario data"""
    try:
        # Reset status
        initialization_status['initialized'] = False
        initialization_status['error'] = None
        scenario_results.clear()

        # Start async initialization
        initialize_scheduler_lazy()

        return jsonify({
            'success': True,
            'message': 'Refresh started',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/teams')
def get_teams():
    """Get list of all teams with their capacities"""
    teams = []

    if scheduler:
        # Add mechanic teams
        for team in scheduler.team_capacity:
            teams.append({
                'id': team,
                'type': 'mechanic',
                'capacity': scheduler.team_capacity[team],
                'shifts': scheduler.team_shifts.get(team, [])
            })

        # Add quality teams
        for team in scheduler.quality_team_capacity:
            teams.append({
                'id': team,
                'type': 'quality',
                'capacity': scheduler.quality_team_capacity[team],
                'shifts': scheduler.quality_team_shifts.get(team, [])
            })

    return jsonify({'teams': teams})


@app.route('/api/holidays')
def get_holidays():
    """Get holiday calendar for all products"""
    if not scheduler:
        return jsonify({'error': 'Scheduler not initialized'}), 500

    holidays_data = {}

    # Convert holiday dates to ISO format strings
    for product, holiday_dates in scheduler.holidays.items():
        holidays_data[product] = [
            date.isoformat() if hasattr(date, 'isoformat') else str(date)
            for date in holiday_dates
        ]

    # Also add weekends as a special category
    # Generate weekends for the schedule period
    if scheduler.task_schedule:
        start_date = min(sched['start_time'] for sched in scheduler.task_schedule.values())
        end_date = max(sched['end_time'] for sched in scheduler.task_schedule.values())

        weekends = []
        current = start_date.date()
        while current <= end_date.date():
            if current.weekday() in [5, 6]:  # Saturday = 5, Sunday = 6
                weekends.append(current.isoformat())
            current += timedelta(days=1)

        holidays_data['_weekends'] = weekends

    return jsonify(holidays_data)


@app.route('/api/mechanics')
def get_mechanics():
    """Get list of all mechanics"""
    # In production, this would come from a database
    mechanics = [
        {'id': 'mech1', 'name': 'John Smith', 'team': 'Mechanic Team 1'},
        {'id': 'mech2', 'name': 'Jane Doe', 'team': 'Mechanic Team 1'},
        {'id': 'mech3', 'name': 'Bob Johnson', 'team': 'Mechanic Team 2'},
        {'id': 'mech4', 'name': 'Alice Williams', 'team': 'Mechanic Team 2'},
        {'id': 'mech5', 'name': 'Charlie Brown', 'team': 'Mechanic Team 3'},
        {'id': 'mech6', 'name': 'Diana Prince', 'team': 'Mechanic Team 3'},
        {'id': 'mech7', 'name': 'Frank Castle', 'team': 'Mechanic Team 4'},
        {'id': 'mech8', 'name': 'Grace Lee', 'team': 'Mechanic Team 4'},
        {'id': 'qual1', 'name': 'Tom Wilson', 'team': 'Quality Team 1'},
        {'id': 'qual2', 'name': 'Sarah Connor', 'team': 'Quality Team 2'},
        {'id': 'qual3', 'name': 'Mike Ross', 'team': 'Quality Team 3'}
    ]
    return jsonify({'mechanics': mechanics})


@app.route('/api/stats')
def get_statistics():
    """Get overall statistics across all scenarios"""
    stats = {
        'scenarios': {},
        'comparison': {},
        'loading': initialization_status['initializing']
    }

    for scenario_id, data in scenario_results.items():
        stats['scenarios'][scenario_id] = {
            'workforce': data['totalWorkforce'],
            'makespan': data['makespan'],
            'onTimeRate': data['onTimeRate'],
            'utilization': data['avgUtilization'],
            'maxLateness': data.get('maxLateness', 0),
            'totalLateness': data.get('totalLateness', 0),
            'totalTaskInstances': data.get('totalTasks', 0),
            'uniqueTaskNumbers': data.get('totalUniqueTaskNums', 0)
        }

    # Calculate comparisons
    if 'baseline' in scenario_results:
        baseline_workforce = scenario_results['baseline']['totalWorkforce']
        baseline_makespan = scenario_results['baseline']['makespan']

        for scenario_id, data in scenario_results.items():
            if scenario_id != 'baseline':
                workforce_diff = data['totalWorkforce'] - baseline_workforce
                makespan_diff = data['makespan'] - baseline_makespan

                stats['comparison'][scenario_id] = {
                    'workforceDiff': workforce_diff,
                    'workforcePercent': round((workforce_diff / baseline_workforce) * 100,
                                              1) if baseline_workforce > 0 else 0,
                    'makespanDiff': makespan_diff,
                    'makespanPercent': round((makespan_diff / baseline_makespan) * 100,
                                             1) if baseline_makespan > 0 else 0
                }

    return jsonify(stats)


@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'scheduler_loaded': scheduler is not None,
        'scenarios_loaded': len(scenario_results),
        'initializing': initialization_status['initializing'],
        'initialized': initialization_status['initialized'],
        'timestamp': datetime.now().isoformat()
    })


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    try:
        print("\n" + "=" * 80)
        print("Starting Production Scheduling Dashboard Server")
        print("=" * 80)
        print("\nServer starting on: http://localhost:5000")
        print("Dashboard will be available immediately.")
        print("Scenario calculations will begin in the background after first access.")
        print("-" * 80 + "\n")

        # Run Flask app (scheduler will initialize on first request)
        app.run(debug=True, host='0.0.0.0', port=5000)

    except Exception as e:
        print(f"\n✗ Failed to start server: {str(e)}")
        traceback.print_exc()