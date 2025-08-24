"""
Enhanced Production Scheduler with Product-Task Instance Model

Key Changes:
1. Tasks are now templates that get instantiated per product
2. Each product-task combination is a unique schedulable entity
3. Task IDs are now formatted as "PRODUCT_TASKNUM" (e.g., "A_80", "E_25")
4. Dependencies, late parts, and rework are product-specific

Original functionality maintained:
- All three scenarios (CSV capacity, minimize makespan, multi-dimensional optimization)
- Complete Phase 2 optimization in Scenario 3
- Priority simulation capabilities
- All utility methods and validations
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict, deque
import heapq
from typing import Dict, List, Set, Tuple, Optional
import warnings
import copy

warnings.filterwarnings('ignore')


class ProductionScheduler:
    """
    Enhanced Production scheduling system where tasks are templates instantiated per product.

    Key Concept: Tasks 1-100 are standard operations that all products require, but
    each product may have already completed some tasks. The scheduler only handles
    incomplete tasks for each product.

    Each task becomes multiple instances based on which products still need it:
    - Task 80: Needed by all 5 products → creates A_80, B_80, C_80, D_80, E_80
    - Task 50: Needed by products C, D, E → creates C_50, D_50, E_50
    - Task 1: Only needed by product E → creates E_1
    """

    def __init__(self, csv_file_path='scheduling_data.csv', debug=False, late_part_delay_days=1.0):
        """
        Initialize scheduler with product-task instance model.

        Args:
            csv_file_path: Path to the CSV file with scheduling data
            debug: Enable verbose debug output
            late_part_delay_days: Days after on-dock date before late part task can start
        """
        self.csv_path = csv_file_path
        self.debug = debug
        self.late_part_delay_days = late_part_delay_days

        # Template tasks (original task definitions)
        self.task_templates = {}  # task_id -> task definition

        # Product-task instances (actual schedulable entities)
        self.tasks = {}  # product_task_id -> task info

        # Product-specific incomplete tasks
        self.product_incomplete_tasks = defaultdict(list)  # product -> list of task numbers

        # Constraints and relationships
        self.precedence_constraints = []
        self.late_part_constraints = []
        self.rework_constraints = []
        self.late_part_tasks = {}
        self.rework_tasks = {}
        self.on_dock_dates = {}
        self.task_to_product = {}  # For late parts/rework associations

        # Quality requirements
        self.quality_inspections = {}
        self.quality_requirements = {}

        # Teams and resources
        self.team_shifts = {}
        self.team_capacity = {}
        self.quality_team_shifts = {}
        self.quality_team_capacity = {}
        self.shift_hours = {}

        # Scheduling
        self.delivery_dates = {}
        self.holidays = defaultdict(set)
        self.product_tasks = defaultdict(list)
        self.task_schedule = {}
        self.global_priority_list = []

        # Caches
        self._dynamic_constraints_cache = None
        self._critical_path_cache = {}

        # Store originals for reset
        self._original_team_capacity = {}
        self._original_quality_capacity = {}

    def debug_print(self, message, force=False):
        """Print debug message if debug mode is enabled or forced"""
        if self.debug or force:
            print(message)

    def create_product_task_id(self, product, task_num):
        """Create unique ID for product-task instance"""
        # Use product initial for compact IDs
        product_initial = product.replace("Product ", "")
        return f"{product_initial}_{task_num}"

    def parse_product_task_id(self, product_task_id):
        """Parse product-task ID back to components"""
        if not isinstance(product_task_id, str):
            return None, None
        parts = product_task_id.split('_')
        if len(parts) == 2:
            product = f"Product {parts[0]}"
            try:
                task_num = int(parts[1])
                return product, task_num
            except ValueError:
                return None, None
        return None, None

    def parse_csv_sections(self, file_content):
        """Parse CSV file content into separate sections based on ==== markers"""
        sections = {}
        current_section = None
        current_data = []

        for line in file_content.strip().split('\n'):
            if '====' in line and line.strip().startswith('===='):
                if current_section and current_data:
                    sections[current_section] = '\n'.join(current_data)
                    if self.debug:
                        print(f"[DEBUG] Saved section '{current_section}' with {len(current_data)} lines")
                current_section = line.replace('=', '').strip()
                current_data = []
            else:
                if line.strip():
                    current_data.append(line)

        if current_section and current_data:
            sections[current_section] = '\n'.join(current_data)
            if self.debug:
                print(f"[DEBUG] Saved section '{current_section}' with {len(current_data)} lines")

        if self.debug:
            print("\n[DEBUG] Section contents preview:")
            for name, content in sections.items():
                print(f"  '{name}': {repr(content[:100])}...")

        return sections

    def load_data_from_csv(self):
        """Load and instantiate product-specific tasks"""
        print(f"\n[DEBUG] Starting to load data with product-task instance model...")

        # Clear any cached data
        self._dynamic_constraints_cache = None
        self._critical_path_cache = {}

        # Read the CSV file
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            print("[WARNING] UTF-8 decoding failed, trying latin-1...")
            with open(self.csv_path, 'r', encoding='latin-1') as f:
                content = f.read()

        # Remove BOM if present
        if content.startswith('\ufeff'):
            print("[WARNING] Removing BOM from file")
            content = content[1:]

        print(f"[DEBUG] Read {len(content)} characters from CSV file")

        sections = self.parse_csv_sections(content)
        print(f"[DEBUG] Found {len(sections)} sections in CSV file")
        print(f"[DEBUG] Section names found: {list(sections.keys())}")

        # 1. Load task templates first
        self._load_task_templates(sections)

        # 2. Load product incomplete tasks
        self._load_product_incomplete_tasks(sections)

        # 3. Create product-task instances
        self._create_product_task_instances()

        # 4. Load and apply constraints
        self._load_constraints(sections)

        # 5. Load resources and other data
        self._load_resources(sections)

        # Summary
        self._print_loading_summary()

    def _load_task_templates(self, sections):
        """Load task templates from TASK DURATION AND RESOURCE TABLE"""
        if "TASK DURATION AND RESOURCE TABLE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["TASK DURATION AND RESOURCE TABLE"]))
            df.columns = df.columns.str.strip()

            task_count = 0
            for _, row in df.iterrows():
                try:
                    task_id = int(row['Task'])
                    # Check if all required columns are present
                    if pd.isna(row.get('Duration (minutes)')) or pd.isna(row.get('Resource Type')) or pd.isna(row.get('Mechanics Required')):
                        print(f"[WARNING] Skipping incomplete task row: {row}")
                        continue

                    self.task_templates[task_id] = {
                        'duration': int(row['Duration (minutes)']),
                        'team': row['Resource Type'].strip(),
                        'mechanics_required': int(row['Mechanics Required']),
                        'is_quality': False,
                        'task_type': 'Production'
                    }
                    task_count += 1
                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing task row: {row}, Error: {e}")
                    continue
            print(f"[DEBUG] Loaded {task_count} production task templates")

    def _load_product_incomplete_tasks(self, sections):
        """Load which tasks are incomplete for each product"""
        if "PRODUCT LINE JOBS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["PRODUCT LINE JOBS"]))
            df.columns = df.columns.str.strip()

            for _, row in df.iterrows():
                product = row['Product Line'].strip()
                start_task = int(row['Task Start'])
                end_task = int(row['Task End'])

                # Tasks from start to end are incomplete for this product
                incomplete_tasks = list(range(start_task, end_task + 1))
                self.product_incomplete_tasks[product] = incomplete_tasks

                print(f"[DEBUG] {product}: {len(incomplete_tasks)} incomplete tasks ({start_task}-{end_task})")

    def _create_product_task_instances(self):
        """Create individual task instances for each product's incomplete tasks"""
        total_instances = 0

        for product, incomplete_tasks in self.product_incomplete_tasks.items():
            for task_num in incomplete_tasks:
                if task_num not in self.task_templates:
                    continue

                # Create unique product-task ID
                product_task_id = self.create_product_task_id(product, task_num)

                # Copy template and add product info
                template = self.task_templates[task_num].copy()
                template['product_line'] = product
                template['original_task_num'] = task_num

                self.tasks[product_task_id] = template
                self.product_tasks[product].append(product_task_id)
                total_instances += 1

        print(f"[DEBUG] Created {total_instances} product-task instances from {len(self.task_templates)} templates")

        # Show breakdown by product
        for product in sorted(self.product_tasks.keys()):
            print(f"  {product}: {len(self.product_tasks[product])} task instances")

    def _load_constraints(self, sections):
        """Load constraints and apply to product-task instances"""

        # Load precedence constraints (baseline tasks)
        if "TASK RELATIONSHIPS TABLE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["TASK RELATIONSHIPS TABLE"]))
            df.columns = df.columns.str.strip()

            # For each constraint, create product-specific versions
            for _, row in df.iterrows():
                first_task = int(row['First'])
                second_task = int(row['Second'])
                relationship = row.get('Relationship Type', row.get('Relationship', 'Finish <= Start'))

                # Apply to each product that has BOTH tasks incomplete
                for product, incomplete in self.product_incomplete_tasks.items():
                    if first_task in incomplete and second_task in incomplete:
                        first_id = self.create_product_task_id(product, first_task)
                        second_id = self.create_product_task_id(product, second_task)

                        # Verify both tasks actually exist before creating constraint
                        if first_id in self.tasks and second_id in self.tasks:
                            self.precedence_constraints.append({
                                'First': first_id,
                                'Second': second_id,
                                'Relationship': relationship,
                                'Product': product
                            })

            print(f"[DEBUG] Created {len(self.precedence_constraints)} product-specific precedence constraints")

        # Load late parts (product-specific)
        if "LATE PARTS RELATIONSHIPS TABLE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["LATE PARTS RELATIONSHIPS TABLE"]))
            df.columns = df.columns.str.strip()

            lp_count = 0
            has_product_column = 'Product Line' in df.columns

            if not has_product_column:
                print(f"[WARNING] No 'Product Line' column in LATE PARTS RELATIONSHIPS TABLE")
                print(f"[WARNING] Late parts will be associated with products based on dependent tasks")

            for _, row in df.iterrows():
                try:
                    first_task = int(row['First'])  # Late part task
                    second_task = int(row['Second'])  # Dependent task
                    on_dock_date = pd.to_datetime(row['Estimated On Dock Date'])
                    product_line = row['Product Line'].strip() if has_product_column and pd.notna(
                        row.get('Product Line')) else None

                    if product_line:
                        # Check if the dependent task exists for this product
                        incomplete_tasks = self.product_incomplete_tasks.get(product_line, [])
                        if second_task in incomplete_tasks:
                            first_id = self.create_product_task_id(product_line, first_task)
                            second_id = self.create_product_task_id(product_line, second_task)

                            # Verify the dependent task exists
                            if second_id in self.tasks:
                                self.late_part_constraints.append({
                                    'First': first_id,
                                    'Second': second_id,
                                    'On_Dock_Date': on_dock_date,
                                    'Product_Line': product_line,
                                    'Relationship': 'Finish <= Start'
                                })

                                self.on_dock_dates[first_id] = on_dock_date
                                self.late_part_tasks[first_id] = True
                                self.task_to_product[first_id] = product_line
                                lp_count += 1
                    else:
                        # No explicit product - infer from dependent task
                        for product, incomplete in self.product_incomplete_tasks.items():
                            if second_task in incomplete:
                                first_id = self.create_product_task_id(product, first_task)
                                second_id = self.create_product_task_id(product, second_task)

                                # Verify the dependent task exists
                                if second_id in self.tasks:
                                    self.late_part_constraints.append({
                                        'First': first_id,
                                        'Second': second_id,
                                        'On_Dock_Date': on_dock_date,
                                        'Product_Line': product,
                                        'Relationship': 'Finish <= Start'
                                    })

                                    self.on_dock_dates[first_id] = on_dock_date
                                    self.late_part_tasks[first_id] = True
                                    self.task_to_product[first_id] = product
                                    lp_count += 1

                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing late part relationship: {e}")

            print(f"[DEBUG] Loaded {lp_count} late part constraints")

            # Show product associations if available
            if self.late_part_constraints:
                product_counts = defaultdict(int)
                for lp in self.late_part_constraints:
                    if lp.get('Product_Line'):
                        product_counts[lp['Product_Line']] += 1
                if product_counts:
                    print(f"[DEBUG] Late parts by product:")
                    for product, count in sorted(product_counts.items()):
                        print(f"  - {product}: {count} late parts")

        # Load late part task details
        if "LATE PARTS TASK DETAILS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["LATE PARTS TASK DETAILS"]))
            df.columns = df.columns.str.strip()

            lp_task_count = 0
            for _, row in df.iterrows():
                try:
                    task_num = int(row['Task'])

                    # Create instances for each product that has this late part
                    for lp_constraint in self.late_part_constraints:
                        product, lp_task_num = self.parse_product_task_id(lp_constraint['First'])
                        if lp_task_num == task_num and product:
                            product_task_id = self.create_product_task_id(product, task_num)

                            # Only create if not already exists
                            if product_task_id not in self.tasks:
                                self.tasks[product_task_id] = {
                                    'duration': int(row['Duration (minutes)']),
                                    'team': row['Resource Type'].strip(),
                                    'mechanics_required': int(row['Mechanics Required']),
                                    'is_quality': False,
                                    'task_type': 'Late Part',
                                    'product_line': product,
                                    'original_task_num': task_num
                                }
                                lp_task_count += 1

                                if product_task_id not in self.product_tasks[product]:
                                    self.product_tasks[product].append(product_task_id)

                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing late part task details: {e}")

            print(f"[DEBUG] Added {lp_task_count} late part task details")

        # Load rework constraints
        self._load_rework_constraints(sections)

        # Create quality inspections
        self._create_quality_inspections(sections)

    def _load_rework_constraints(self, sections):
        """Load rework relationships and tasks"""
        if "REWORK RELATIONSHIPS TABLE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["REWORK RELATIONSHIPS TABLE"]))
            df.columns = df.columns.str.strip()

            rw_count = 0
            has_product_column = 'Product Line' in df.columns

            if not has_product_column:
                print(f"[WARNING] No 'Product Line' column in REWORK RELATIONSHIPS TABLE")

            for _, row in df.iterrows():
                try:
                    first_task = int(row['First'])  # Rework task
                    second_task = int(row['Second'])  # Dependent task
                    relationship = row.get('Relationship Type', 'Finish <= Start').strip() if pd.notna(
                        row.get('Relationship Type')) else 'Finish <= Start'
                    product_line = row['Product Line'].strip() if has_product_column and pd.notna(
                        row.get('Product Line')) else None

                    if product_line:
                        # Check if the dependent task exists for this product
                        incomplete_tasks = self.product_incomplete_tasks.get(product_line, [])

                        # The dependent task might be another rework task, so check both incomplete and rework
                        second_id = self.create_product_task_id(product_line, second_task)

                        # Only create constraint if the second task will exist (either as regular or rework)
                        if second_task in incomplete_tasks or second_id in self.rework_tasks:
                            first_id = self.create_product_task_id(product_line, first_task)

                            self.rework_constraints.append({
                                'First': first_id,
                                'Second': second_id,
                                'Relationship': relationship,
                                'Product_Line': product_line
                            })

                            self.rework_tasks[first_id] = True
                            self.task_to_product[first_id] = product_line
                            rw_count += 1
                    else:
                        # Infer from dependent task
                        for product, incomplete in self.product_incomplete_tasks.items():
                            second_id = self.create_product_task_id(product, second_task)

                            # Check if dependent task exists or will exist
                            if second_task in incomplete or second_id in self.rework_tasks:
                                first_id = self.create_product_task_id(product, first_task)

                                self.rework_constraints.append({
                                    'First': first_id,
                                    'Second': second_id,
                                    'Relationship': relationship,
                                    'Product_Line': product
                                })

                                self.rework_tasks[first_id] = True
                                self.task_to_product[first_id] = product
                                rw_count += 1

                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing rework relationship: {e}")

            print(f"[DEBUG] Loaded {rw_count} rework relationships")

            # Show product associations
            if self.rework_constraints:
                product_counts = defaultdict(int)
                for rw in self.rework_constraints:
                    if rw.get('Product_Line'):
                        product_counts[rw['Product_Line']] += 1
                if product_counts:
                    print(f"[DEBUG] Rework by product:")
                    for product, count in sorted(product_counts.items()):
                        print(f"  - {product}: {count} rework tasks")

        # Load rework task details
        if "REWORK TASK DETAILS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["REWORK TASK DETAILS"]))
            df.columns = df.columns.str.strip()

            rw_task_count = 0
            for _, row in df.iterrows():
                try:
                    task_num = int(row['Task'])

                    # Create instances for products that need this rework
                    for rw_constraint in self.rework_constraints:
                        product, rw_task_num = self.parse_product_task_id(rw_constraint['First'])
                        if rw_task_num == task_num and product:
                            product_task_id = self.create_product_task_id(product, task_num)

                            # Only create if not already exists
                            if product_task_id not in self.tasks:
                                self.tasks[product_task_id] = {
                                    'duration': int(row['Duration (minutes)']),
                                    'team': row['Resource Type'].strip(),
                                    'mechanics_required': int(row['Mechanics Required']),
                                    'is_quality': False,
                                    'task_type': 'Rework',
                                    'product_line': product,
                                    'original_task_num': task_num
                                }
                                rw_task_count += 1

                                if product_task_id not in self.product_tasks[product]:
                                    self.product_tasks[product].append(product_task_id)

                                # Create quality inspection for rework
                                qi_task_id = self.create_product_task_id(product, task_num + 10000)
                                self.quality_requirements[product_task_id] = qi_task_id

                                self.tasks[qi_task_id] = {
                                    'duration': 30,
                                    'team': None,
                                    'mechanics_required': 1,
                                    'is_quality': True,
                                    'task_type': 'Quality Inspection',
                                    'primary_task': product_task_id,
                                    'product_line': product,
                                    'original_task_num': task_num + 10000
                                }

                                self.quality_inspections[qi_task_id] = {
                                    'primary_task': product_task_id,
                                    'headcount': 1
                                }

                                if qi_task_id not in self.product_tasks[product]:
                                    self.product_tasks[product].append(qi_task_id)
                                    self.task_to_product[qi_task_id] = product

                except (ValueError, KeyError) as e:
                    print(f"[WARNING] Error processing rework task details: {e}")

            print(f"[DEBUG] Loaded {rw_task_count} rework task details")
            if rw_task_count > 0:
                print(f"[DEBUG] Created {rw_task_count} quality inspections for rework tasks")

    def _create_quality_inspections(self, sections):
        """Create quality inspection tasks for production tasks"""
        if "QUALITY INSPECTION REQUIREMENTS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["QUALITY INSPECTION REQUIREMENTS"]))
            df.columns = df.columns.str.strip()

            qi_count = 0
            for _, row in df.iterrows():
                primary_task_num = int(row['Primary Task'])
                qi_task_num = int(row['Quality Task'])
                duration = int(row['Quality Duration (minutes)'])
                headcount = int(row['Quality Headcount Required'])

                # Create QI for each product that has this task incomplete
                for product, incomplete in self.product_incomplete_tasks.items():
                    if primary_task_num in incomplete:
                        primary_id = self.create_product_task_id(product, primary_task_num)
                        qi_id = self.create_product_task_id(product, qi_task_num)

                        # Only create QI if the primary task exists
                        if primary_id in self.tasks:
                            self.tasks[qi_id] = {
                                'duration': duration,
                                'team': None,  # Will be assigned during scheduling
                                'mechanics_required': headcount,
                                'is_quality': True,
                                'task_type': 'Quality Inspection',
                                'product_line': product,
                                'original_task_num': qi_task_num,
                                'primary_task': primary_id
                            }

                            self.quality_requirements[primary_id] = qi_id
                            self.quality_inspections[qi_id] = {
                                'primary_task': primary_id,
                                'headcount': headcount
                            }

                            if qi_id not in self.product_tasks[product]:
                                self.product_tasks[product].append(qi_id)

                            qi_count += 1

            print(f"[DEBUG] Created {qi_count} quality inspection instances for baseline tasks")
            print(f"[DEBUG] Total tasks now: {len(self.tasks)}")

    def _load_resources(self, sections):
        """Load team capacities, shifts, holidays, etc."""
        # Mechanic team calendars
        if "MECHANIC TEAM WORKING CALENDARS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["MECHANIC TEAM WORKING CALENDARS"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                team_name = row['Mechanic Team'].strip()
                shifts = row['Working Shifts']
                if 'All 3 shifts' in shifts:
                    self.team_shifts[team_name] = ['1st', '2nd', '3rd']
                elif 'and' in shifts:
                    self.team_shifts[team_name] = [s.strip() for s in shifts.split('and')]
                else:
                    self.team_shifts[team_name] = [shifts.strip()]
            print(f"[DEBUG] Loaded {len(self.team_shifts)} mechanic team schedules")

        # Quality team calendars
        if "QUALITY TEAM WORKING CALENDARS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["QUALITY TEAM WORKING CALENDARS"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                team_name = row['Quality Team'].strip()
                self.quality_team_shifts[team_name] = [row['Working Shifts'].strip()]
            print(f"[DEBUG] Loaded {len(self.quality_team_shifts)} quality team schedules")

        # Shift working hours
        if "SHIFT WORKING HOURS" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["SHIFT WORKING HOURS"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                self.shift_hours[row['Shift'].strip()] = {
                    'start': row['Start Time'].strip(),
                    'end': row['End Time'].strip()
                }
            print(f"[DEBUG] Loaded {len(self.shift_hours)} shift definitions")

        # Mechanic team capacity
        if "MECHANIC TEAM CAPACITY" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["MECHANIC TEAM CAPACITY"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                team_name = row['Mechanic Team'].strip()
                capacity = int(row['Total Capacity (People)'])
                self.team_capacity[team_name] = capacity
                self._original_team_capacity[team_name] = capacity
            print(f"[DEBUG] Loaded capacity for {len(self.team_capacity)} mechanic teams")

        # Quality team capacity
        if "QUALITY TEAM CAPACITY" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["QUALITY TEAM CAPACITY"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                team_name = row['Quality Team'].strip()
                capacity = int(row['Total Capacity (People)'])
                self.quality_team_capacity[team_name] = capacity
                self._original_quality_capacity[team_name] = capacity
            print(f"[DEBUG] Loaded capacity for {len(self.quality_team_capacity)} quality teams")

        # Product delivery schedule
        if "PRODUCT LINE DELIVERY SCHEDULE" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["PRODUCT LINE DELIVERY SCHEDULE"]))
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                product = row['Product Line'].strip()
                self.delivery_dates[product] = pd.to_datetime(row['Delivery Date'])
            print(f"[DEBUG] Loaded delivery dates for {len(self.delivery_dates)} product lines")

        # Holiday calendar
        if "PRODUCT LINE HOLIDAY CALENDAR" in sections:
            from io import StringIO
            df = pd.read_csv(StringIO(sections["PRODUCT LINE HOLIDAY CALENDAR"]))
            df.columns = df.columns.str.strip()
            holiday_count = 0
            for _, row in df.iterrows():
                product = row['Product Line'].strip()
                self.holidays[product].add(pd.to_datetime(row['Date']))
                holiday_count += 1
            print(f"[DEBUG] Loaded {holiday_count} holiday entries")

    def _print_loading_summary(self):
        """Print summary of loaded data"""
        print(f"\n[DEBUG] LOADING SUMMARY:")
        print(f"  Total task instances: {len(self.tasks)}")

        # Count by type
        task_type_counts = defaultdict(int)
        for task_info in self.tasks.values():
            task_type_counts[task_info['task_type']] += 1

        print(f"\n[DEBUG] Task Type Summary:")
        for task_type, count in sorted(task_type_counts.items()):
            print(f"  - {task_type}: {count}")

        # Product-specific breakdown
        print(f"\n[DEBUG] Product-Specific Task Breakdown:")
        for product in sorted(self.product_tasks.keys()):
            tasks_in_product = self.product_tasks[product]
            type_counts = defaultdict(int)
            for task_id in tasks_in_product:
                if task_id in self.tasks:
                    type_counts[self.tasks[task_id]['task_type']] += 1

            print(f"  {product}: {len(tasks_in_product)} total tasks")
            for task_type, count in sorted(type_counts.items()):
                print(f"    - {task_type}: {count}")

        print(f"\n[DEBUG] Data loading complete!")

    def build_dynamic_dependencies(self):
        """Build dependency graph with dynamic quality inspection insertion and product-specific constraints"""
        if self._dynamic_constraints_cache is not None:
            return self._dynamic_constraints_cache

        self.debug_print(f"\n[DEBUG] Building dynamic dependencies...")
        self.debug_print(f"[DEBUG] Original constraints: {len(self.precedence_constraints)}")
        self.debug_print(f"[DEBUG] Late part constraints: {len(self.late_part_constraints)}")
        self.debug_print(f"[DEBUG] Rework constraints: {len(self.rework_constraints)}")
        self.debug_print(f"[DEBUG] Quality requirements: {len(self.quality_requirements)}")

        dynamic_constraints = []

        # 1. Add baseline task constraints with QI redirection - FILTER OUT INVALID CONSTRAINTS
        qi_redirections = 0
        invalid_constraints = 0
        for constraint in self.precedence_constraints:
            first_task = constraint['First']
            second_task = constraint['Second']
            relationship = constraint.get('Relationship', 'Finish <= Start')

            # Check if both tasks actually exist
            if first_task not in self.tasks or second_task not in self.tasks:
                invalid_constraints += 1
                continue  # Skip this constraint

            # Check if first task has quality inspection
            if first_task in self.quality_requirements:
                qi_task = self.quality_requirements[first_task]
                qi_redirections += 1

                # Add constraint from primary task to QI (Finish = Start)
                if not any(c['First'] == first_task and c['Second'] == qi_task
                           for c in dynamic_constraints):
                    dynamic_constraints.append({
                        'First': first_task,
                        'Second': qi_task,
                        'Relationship': 'Finish = Start'
                    })

                # Redirect original constraint through QI
                dynamic_constraints.append({
                    'First': qi_task,
                    'Second': second_task,
                    'Relationship': relationship
                })
            else:
                # No QI, keep original constraint
                dynamic_constraints.append({
                    'First': first_task,
                    'Second': second_task,
                    'Relationship': relationship
                })

        if invalid_constraints > 0:
            self.debug_print(f"[DEBUG] Filtered out {invalid_constraints} invalid constraints")

        # 2. Add late part constraints - FILTER OUT INVALID
        lp_by_product = defaultdict(int)
        invalid_lp = 0
        for lp_constraint in self.late_part_constraints:
            first_task = lp_constraint['First']
            second_task = lp_constraint['Second']
            product = lp_constraint.get('Product_Line', 'Unknown')

            # Check if second task exists (first task will be created as late part)
            if second_task not in self.tasks:
                invalid_lp += 1
                continue

            lp_by_product[product] += 1

            # Late part must finish before primary task starts
            dynamic_constraints.append({
                'First': first_task,
                'Second': second_task,
                'Relationship': 'Finish <= Start',
                'Type': 'Late Part',
                'Product_Line': product
            })

            if self.debug and len(self.late_part_constraints) <= 5:
                print(f"[DEBUG] Added late part constraint: Task {first_task} -> Task {second_task} ({product})")

        if invalid_lp > 0:
            self.debug_print(f"[DEBUG] Filtered out {invalid_lp} invalid late part constraints")

        if lp_by_product and self.debug:
            print(f"[DEBUG] Late part constraints by product: {dict(lp_by_product)}")

        # 3. Add rework constraints (including their QI) - FILTER OUT INVALID
        rw_by_product = defaultdict(int)
        invalid_rw = 0
        for rw_constraint in self.rework_constraints:
            first_task = rw_constraint['First']
            second_task = rw_constraint['Second']
            relationship = rw_constraint.get('Relationship', 'Finish <= Start')
            product = rw_constraint.get('Product_Line', 'Unknown')

            # Check if second task exists or will be created
            if second_task not in self.tasks and second_task not in self.rework_tasks:
                invalid_rw += 1
                continue

            rw_by_product[product] += 1

            # If rework task has QI, redirect through it
            if first_task in self.quality_requirements:
                qi_task = self.quality_requirements[first_task]

                # Add constraint from rework task to its QI
                if not any(c['First'] == first_task and c['Second'] == qi_task
                           for c in dynamic_constraints):
                    dynamic_constraints.append({
                        'First': first_task,
                        'Second': qi_task,
                        'Relationship': 'Finish = Start',
                        'Type': 'Rework QI',
                        'Product_Line': product
                    })

                # Redirect constraint through QI
                dynamic_constraints.append({
                    'First': qi_task,
                    'Second': second_task,
                    'Relationship': relationship,
                    'Type': 'Rework',
                    'Product_Line': product
                })
            else:
                # No QI, direct constraint
                dynamic_constraints.append({
                    'First': first_task,
                    'Second': second_task,
                    'Relationship': relationship,
                    'Type': 'Rework',
                    'Product_Line': product
                })

            if self.debug and len(self.rework_constraints) <= 5:
                print(f"[DEBUG] Added rework constraint: Task {first_task} -> Task {second_task} ({product})")

        if invalid_rw > 0:
            self.debug_print(f"[DEBUG] Filtered out {invalid_rw} invalid rework constraints")

        if rw_by_product and self.debug:
            print(f"[DEBUG] Rework constraints by product: {dict(rw_by_product)}")

        # 4. Add any QI constraints that weren't already added
        added_qi_constraints = 0
        for primary_task, qi_task in self.quality_requirements.items():
            if not any(c['First'] == primary_task and c['Second'] == qi_task
                       for c in dynamic_constraints):
                dynamic_constraints.append({
                    'First': primary_task,
                    'Second': qi_task,
                    'Relationship': 'Finish = Start'
                })
                added_qi_constraints += 1

        self.debug_print(f"[DEBUG] QI redirections: {qi_redirections}")
        self.debug_print(f"[DEBUG] Additional QI constraints added: {added_qi_constraints}")
        self.debug_print(f"[DEBUG] Total dynamic constraints: {len(dynamic_constraints)}")

        self._dynamic_constraints_cache = dynamic_constraints
        return dynamic_constraints

    def get_earliest_start_for_late_part(self, task_id):
        """Calculate earliest start time for a late part task based on on-dock date"""
        if task_id not in self.on_dock_dates:
            return datetime(2025, 8, 22, 6, 0)  # Default start date

        on_dock_date = self.on_dock_dates[task_id]
        # Add the parameterizable delay (default 1 day)
        earliest_start = on_dock_date + timedelta(days=self.late_part_delay_days)

        # Set to start of workday (6 AM)
        earliest_start = earliest_start.replace(hour=6, minute=0, second=0, microsecond=0)

        return earliest_start

    def schedule_tasks(self, allow_late_delivery=False, silent_mode=False):
        """Enhanced scheduling algorithm with capacity awareness and product-task instances"""
        # Save original debug setting
        original_debug = self.debug
        if silent_mode:
            self.debug = False

        # Clear previous schedule
        self.task_schedule = {}
        self._critical_path_cache = {}

        # Validate DAG first
        if not silent_mode and not self.validate_dag():
            raise ValueError("DAG validation failed! Cannot proceed with scheduling.")

        # Build dynamic dependencies including quality inspections, late parts, and rework
        dynamic_constraints = self.build_dynamic_dependencies()

        # Initialize start date
        start_date = datetime(2025, 8, 22, 6, 0)  # Start at 6 AM

        # Create dependency graph
        dependencies = defaultdict(set)
        dependents = defaultdict(set)

        for constraint in dynamic_constraints:
            if constraint['Relationship'] in ['Finish <= Start', 'Finish = Start']:
                dependencies[constraint['Second']].add(constraint['First'])
                dependents[constraint['First']].add(constraint['Second'])
            elif constraint['Relationship'] == 'Start <= Start':
                dependencies[constraint['Second']].add(constraint['First'])
                dependents[constraint['First']].add(constraint['Second'])

        # Find tasks with no dependencies (can start immediately)
        all_tasks = set(self.tasks.keys())
        total_tasks = len(all_tasks)
        ready_tasks = []

        if not silent_mode:
            print(f"\nStarting scheduling for {total_tasks} total task instances...")
            # Count instances per product
            instances_per_product = defaultdict(int)
            for task_id in all_tasks:
                product, _ = self.parse_product_task_id(task_id)
                if product:
                    instances_per_product[product] += 1
            for product, count in sorted(instances_per_product.items()):
                print(f"- {product}: {count} instances")

        for task in all_tasks:
            if task not in dependencies or len(dependencies[task]) == 0:
                priority = self.calculate_task_priority(task)
                heapq.heappush(ready_tasks, (priority, task))

        if not silent_mode:
            print(f"- Initial ready tasks: {len(ready_tasks)}")

        # Schedule tasks
        scheduled_count = 0
        current_time = start_date
        max_retries = 5
        retry_count = 0
        failed_tasks = set()
        task_retry_counts = defaultdict(int)
        max_iterations = total_tasks * 10
        iteration_count = 0

        while (ready_tasks or scheduled_count < total_tasks) and retry_count < max_retries and iteration_count < max_iterations:
            iteration_count += 1

            if not ready_tasks and scheduled_count + len(failed_tasks) < total_tasks:
                if not silent_mode:
                    print(f"\n[DEBUG] No ready tasks but {total_tasks - scheduled_count - len(failed_tasks)} tasks remain unscheduled")
                unscheduled = [t for t in all_tasks if t not in self.task_schedule and t not in failed_tasks]

                newly_ready = []
                for task in unscheduled:
                    if task in failed_tasks:
                        continue
                    deps = dependencies.get(task, set())
                    unscheduled_deps = [d for d in deps if d not in self.task_schedule and d not in failed_tasks]
                    if len(unscheduled_deps) == 0:
                        priority = self.calculate_task_priority(task)
                        heapq.heappush(ready_tasks, (priority, task))
                        newly_ready.append(task)

                if newly_ready and not silent_mode:
                    print(f"[DEBUG] Found {len(newly_ready)} newly ready tasks")
                elif not newly_ready and not silent_mode:
                    print(f"\n[ERROR] No more tasks can be scheduled")
                    break

            if not ready_tasks:
                if not silent_mode:
                    print(f"[ERROR] Ready task queue is empty unexpectedly!")
                break

            priority, task_id = heapq.heappop(ready_tasks)

            # Check if this task has failed too many times
            if task_retry_counts[task_id] >= 3:
                if task_id not in failed_tasks:
                    failed_tasks.add(task_id)
                    if not silent_mode:
                        print(f"[WARNING] Task {task_id} failed too many times, skipping permanently")
                continue

            if scheduled_count % 50 == 0 and not silent_mode:
                task_type = self.tasks[task_id]['task_type']
                product, task_num = self.parse_product_task_id(task_id)
                print(f"\n[DEBUG] Scheduling {task_id} ({product} Task {task_num}, {task_type}, priority: {priority:.1f})")

            # Get product line for this task
            product_line = self.tasks[task_id].get('product_line')
            if not product_line:
                product, _ = self.parse_product_task_id(task_id)
                product_line = product

            if not product_line:
                if not silent_mode:
                    print(f"[WARNING] No product line found for task {task_id} - skipping")
                continue

            # Get task details
            task_info = self.tasks[task_id]
            duration = task_info['duration']
            mechanics_needed = task_info['mechanics_required']
            is_quality = task_info['is_quality']
            task_type = task_info['task_type']

            # Find earliest available time considering dependencies
            earliest_start = current_time

            # Special handling for late part tasks - respect on-dock date
            if task_id in self.late_part_tasks:
                late_part_earliest = self.get_earliest_start_for_late_part(task_id)
                earliest_start = max(earliest_start, late_part_earliest)
                if scheduled_count % 50 == 0 and not silent_mode:
                    print(f"[DEBUG]   Late part task, earliest start after on-dock: {late_part_earliest}")

            # Check dependency constraints
            constraint_count = 0
            for dep in dependencies.get(task_id, set()):
                if dep in self.task_schedule:
                    dep_end = self.task_schedule[dep]['end_time']
                    constraint_count += 1

                    # Check if this is a Finish = Start relationship
                    is_finish_equals_start = False
                    for constraint in dynamic_constraints:
                        if (constraint['First'] == dep and
                            constraint['Second'] == task_id and
                            constraint['Relationship'] == 'Finish = Start'):
                            is_finish_equals_start = True
                            break

                    if is_finish_equals_start:
                        earliest_start = dep_end
                    else:
                        earliest_start = max(earliest_start, dep_end)

            if scheduled_count % 50 == 0 and constraint_count > 0 and not silent_mode:
                print(f"[DEBUG]   Constrained by {constraint_count} dependencies, earliest start: {earliest_start}")

            # Find next available working time with capacity
            if is_quality:
                # Try to find a quality team with capacity
                scheduled_start = None
                team = None
                shift = None

                for try_shift in ['1st', '2nd', '3rd']:
                    temp_team = self.assign_quality_team_balanced(try_shift, mechanics_needed)
                    if temp_team:
                        try:
                            temp_start, _ = self.get_next_working_time_with_capacity(
                                earliest_start, product_line, temp_team, mechanics_needed,
                                duration, is_quality=True)
                            if not scheduled_start or temp_start < scheduled_start:
                                scheduled_start = temp_start
                                team = temp_team
                                shift = try_shift
                        except:
                            continue

                if not team:
                    task_retry_counts[task_id] += 1
                    if task_retry_counts[task_id] < 3:
                        heapq.heappush(ready_tasks, (priority + 0.1, task_id))
                    else:
                        failed_tasks.add(task_id)
                    continue
            else:
                team = task_info['team']
                try:
                    scheduled_start, shift = self.get_next_working_time_with_capacity(
                        earliest_start, product_line, team, mechanics_needed,
                        duration, is_quality=False)
                except Exception as e:
                    task_retry_counts[task_id] += 1
                    if task_retry_counts[task_id] < 3:
                        heapq.heappush(ready_tasks, (priority + 0.1, task_id))
                    else:
                        failed_tasks.add(task_id)
                    continue

            # Schedule the task
            scheduled_end = scheduled_start + timedelta(minutes=int(duration))

            self.task_schedule[task_id] = {
                'start_time': scheduled_start,
                'end_time': scheduled_end,
                'team': team,
                'product_line': product_line,
                'duration': duration,
                'mechanics_required': mechanics_needed,
                'is_quality': is_quality,
                'task_type': task_type,
                'shift': shift
            }

            scheduled_count += 1
            retry_count = 0

            if scheduled_count % 50 == 0 and not silent_mode:
                print(f"[DEBUG]   Scheduled: {scheduled_start.strftime('%Y-%m-%d %H:%M')} - {scheduled_end.strftime('%H:%M')} ({team}, {shift} shift)")

            # Progress reporting
            if scheduled_count % 100 == 0 and not silent_mode:
                print(f"\n[PROGRESS] {scheduled_count}/{total_tasks} tasks scheduled ({scheduled_count/total_tasks*100:.1f}%)")

            # Add newly ready tasks
            newly_ready = []
            for dependent in dependents.get(task_id, set()):
                if dependent in self.task_schedule or dependent in failed_tasks:
                    continue
                deps = dependencies.get(dependent, set())
                if all(d in self.task_schedule or d in failed_tasks for d in deps):
                    priority = self.calculate_task_priority(dependent)
                    heapq.heappush(ready_tasks, (priority, dependent))
                    newly_ready.append(dependent)

        if not silent_mode:
            print(f"\n[DEBUG] Scheduling complete! Scheduled {scheduled_count}/{total_tasks} task instances.")

            # Report scheduled instances by product
            scheduled_by_product = defaultdict(int)
            for task_id in self.task_schedule:
                product, _ = self.parse_product_task_id(task_id)
                if product:
                    scheduled_by_product[product] += 1

            print("\n[DEBUG] Scheduled instances by product:")
            for product in sorted(scheduled_by_product.keys()):
                total = len(self.product_tasks[product])
                scheduled = scheduled_by_product[product]
                print(f"  {product}: {scheduled}/{total} ({scheduled/total*100:.1f}%)")

            # Report task type breakdown
            scheduled_by_type = defaultdict(int)
            for task_id in self.task_schedule:
                scheduled_by_type[self.tasks[task_id]['task_type']] += 1

            print("\n[DEBUG] Scheduled tasks by type:")
            for task_type, count in sorted(scheduled_by_type.items()):
                total_of_type = sum(1 for t in self.tasks.values() if t['task_type'] == task_type)
                print(f"  - {task_type}: {count}/{total_of_type}")

        # Restore original debug setting
        self.debug = original_debug

    def validate_dag(self):
        """Validate the DAG for cycles and other issues"""
        print("\nValidating task dependency graph...")

        dynamic_constraints = self.build_dynamic_dependencies()

        # Create adjacency list for cycle detection
        graph = defaultdict(set)
        all_tasks_in_constraints = set()

        for constraint in dynamic_constraints:
            first = constraint['First']
            second = constraint['Second']
            graph[first].add(second)
            all_tasks_in_constraints.add(first)
            all_tasks_in_constraints.add(second)

        # Check if all tasks in constraints exist in task list
        missing_tasks = all_tasks_in_constraints - set(self.tasks.keys())
        if missing_tasks:
            print(f"ERROR: Tasks referenced in constraints but not defined: {missing_tasks}")
            return False

        # Validate product associations for late parts and rework
        print("\nValidating product associations...")
        orphan_late_parts = []
        orphan_rework = []

        for task_id in self.late_part_tasks:
            found_in_product = False
            for product, tasks in self.product_tasks.items():
                if task_id in tasks:
                    found_in_product = True
                    break
            if not found_in_product:
                orphan_late_parts.append(task_id)

        for task_id in self.rework_tasks:
            found_in_product = False
            for product, tasks in self.product_tasks.items():
                if task_id in tasks:
                    found_in_product = True
                    break
            if not found_in_product:
                orphan_rework.append(task_id)

        if orphan_late_parts:
            print(f"WARNING: Late part tasks not associated with any product: {orphan_late_parts}")
        if orphan_rework:
            print(f"WARNING: Rework tasks not associated with any product: {orphan_rework}")

        # Detect cycles using DFS
        def has_cycle_dfs(node, visited, rec_stack, path):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle_dfs(neighbor, visited, rec_stack, path):
                        return True
                elif neighbor in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    print(f"ERROR: Cycle detected: {' -> '.join(map(str, cycle))}")
                    return True

            path.pop()
            rec_stack.remove(node)
            return False

        # Check for cycles
        visited = set()
        for node in all_tasks_in_constraints:
            if node not in visited:
                if has_cycle_dfs(node, visited, set(), []):
                    return False

        # Check for unreachable tasks
        all_tasks = set(self.tasks.keys())
        reachable = set()

        # Find root tasks (no predecessors)
        root_tasks = set()
        for task in all_tasks:
            has_predecessor = False
            for constraint in dynamic_constraints:
                if constraint['Second'] == task:
                    has_predecessor = True
                    break
            if not has_predecessor:
                root_tasks.add(task)

        # BFS from root tasks to find all reachable tasks
        queue = deque(root_tasks)
        reachable.update(root_tasks)

        while queue:
            current = queue.popleft()
            for neighbor in graph.get(current, []):
                if neighbor not in reachable:
                    reachable.add(neighbor)
                    queue.append(neighbor)

        unreachable = all_tasks - reachable
        if unreachable:
            print(f"WARNING: {len(unreachable)} tasks are unreachable from root tasks")

        # Summary statistics with task type breakdown
        print(f"\nDAG Validation Summary:")

        task_type_counts = defaultdict(int)
        for task_id in all_tasks:
            task_type_counts[self.tasks[task_id]['task_type']] += 1

        print(f"- Total task instances: {len(all_tasks)}")
        for task_type, count in sorted(task_type_counts.items()):
            print(f"  • {task_type}: {count}")

        print(f"- Total constraints: {len(dynamic_constraints)}")
        print(f"- Root tasks (no dependencies): {len(root_tasks)}")
        print(f"- Reachable tasks: {len(reachable)}")

        print("\nDAG validation completed successfully!")
        return True

    def is_working_day(self, date, product_line):
        """Check if a date is a working day for a specific product line"""
        if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False

        if date.date() in [h.date() for h in self.holidays[product_line]]:
            return False

        return True

    def check_team_capacity_at_time(self, team, start_time, end_time, mechanics_needed):
        """Check if team has available capacity during the specified time period"""
        capacity = self.team_capacity.get(team, 0) or self.quality_team_capacity.get(team, 0)

        # Get all tasks scheduled for this team
        team_tasks = [(task_id, sched) for task_id, sched in self.task_schedule.items()
                     if sched['team'] == team]

        # Check capacity at every minute in the time range
        current = start_time
        while current < end_time:
            usage = 0
            for task_id, sched in team_tasks:
                if sched['start_time'] <= current < sched['end_time']:
                    usage += sched['mechanics_required']

            if usage + mechanics_needed > capacity:
                return False

            current += timedelta(minutes=1)

        return True

    def get_next_working_time_with_capacity(self, current_time, product_line, team, mechanics_needed, duration, is_quality=False):
        """Get the next available working time when team has capacity for the task"""
        max_iterations = 5000
        iterations = 0

        while iterations < max_iterations:
            iterations += 1

            # Check if current day is working day
            if not self.is_working_day(current_time, product_line):
                current_time = current_time.replace(hour=6, minute=0, second=0)
                current_time += timedelta(days=1)
                continue

            # Check team shifts
            current_minutes = current_time.hour * 60 + current_time.minute
            available_shift = None

            if is_quality:
                # For quality teams, check quality team shifts
                for shift in ['1st', '2nd', '3rd']:
                    if shift == '1st' and 360 <= current_minutes < 870:
                        if any(t for t, shifts in self.quality_team_shifts.items()
                              if shift in shifts):
                            available_shift = shift
                            break
                    elif shift == '2nd' and 870 <= current_minutes < 1380:
                        if any(t for t, shifts in self.quality_team_shifts.items()
                              if shift in shifts):
                            available_shift = shift
                            break
                    elif shift == '3rd' and (current_minutes >= 1380 or current_minutes < 360):
                        if any(t for t, shifts in self.quality_team_shifts.items()
                              if shift in shifts):
                            available_shift = shift
                            break
            else:
                # Regular mechanic teams
                for shift in self.team_shifts.get(team, []):
                    if shift == '1st' and 360 <= current_minutes < 870:
                        available_shift = shift
                        break
                    elif shift == '2nd' and 870 <= current_minutes < 1380:
                        available_shift = shift
                        break
                    elif shift == '3rd' and (current_minutes >= 1380 or current_minutes < 360):
                        available_shift = shift
                        break

            if available_shift:
                # Check if team has capacity for this task
                end_time = current_time + timedelta(minutes=duration)
                if self.check_team_capacity_at_time(team, current_time, end_time, mechanics_needed):
                    return current_time, available_shift
                else:
                    # Move to next minute and try again
                    current_time += timedelta(minutes=1)
            else:
                # Move to next available shift
                if current_minutes < 360:
                    current_time = current_time.replace(hour=6, minute=0, second=0)
                elif current_minutes < 870:
                    current_time = current_time.replace(hour=14, minute=30, second=0)
                elif current_minutes < 1380:
                    current_time = current_time.replace(hour=23, minute=0, second=0)
                else:
                    current_time = current_time.replace(hour=6, minute=0, second=0)
                    current_time += timedelta(days=1)

        raise RuntimeError(f"[ERROR] Could not find working time with capacity after {max_iterations} iterations!")

    def assign_quality_team_balanced(self, shift, mechanics_needed):
        """Assign quality team with load balancing"""
        available_teams = [team for team, shifts in self.quality_team_shifts.items()
                          if shift in shifts]

        if not available_teams:
            return None

        # Calculate current load for each team
        team_loads = {}
        for team in available_teams:
            # Check if team has capacity
            capacity = self.quality_team_capacity.get(team, 0)
            if capacity < mechanics_needed:
                continue

            scheduled_minutes = sum(
                sched['duration'] * sched['mechanics_required']
                for sched in self.task_schedule.values()
                if sched['team'] == team
            )
            team_loads[team] = scheduled_minutes

        if not team_loads:
            return None

        # Return team with lowest load
        best_team = min(team_loads.items(), key=lambda x: x[1])[0]
        return best_team

    def calculate_critical_path_length(self, task_id):
        """Calculate the length of the critical path from this task to end"""
        if task_id in self._critical_path_cache:
            return self._critical_path_cache[task_id]

        dynamic_constraints = self.build_dynamic_dependencies()

        def get_path_length(task):
            if task in self._critical_path_cache:
                return self._critical_path_cache[task]

            max_successor_path = 0
            task_duration = self.tasks[task]['duration']

            # Find all successors
            for constraint in dynamic_constraints:
                if constraint['First'] == task:
                    successor = constraint['Second']
                    if successor in self.tasks:  # Ensure successor exists
                        successor_path = get_path_length(successor)
                        max_successor_path = max(max_successor_path, successor_path)

            self._critical_path_cache[task] = task_duration + max_successor_path
            return self._critical_path_cache[task]

        return get_path_length(task_id)

    def calculate_task_priority(self, task_id):
        """Enhanced priority calculation with task type and product-specific considerations"""
        # Late part tasks get high priority to avoid blocking downstream work
        if task_id in self.late_part_tasks:
            return -2000

        # Quality inspections get high priority to minimize gaps
        if task_id in self.quality_inspections:
            return -1000

        # Rework tasks get moderately high priority
        if task_id in self.rework_tasks:
            return -500

        # Get product line
        product_line = None

        # Check explicit product associations first
        if task_id in self.task_to_product:
            product_line = self.task_to_product[task_id]
        elif task_id in self.tasks:
            product_line = self.tasks[task_id].get('product_line')

        if not product_line:
            product, _ = self.parse_product_task_id(task_id)
            product_line = product

        if not product_line:
            return 999999

        # 1. Delivery date urgency
        delivery_date = self.delivery_dates.get(product_line)
        if not delivery_date:
            return 999999

        days_to_delivery = (delivery_date - datetime.now()).days

        # 2. Critical path length from this task
        critical_path_length = self.calculate_critical_path_length(task_id)

        # 3. Number of direct dependent tasks
        dynamic_constraints = self.build_dynamic_dependencies()
        dependent_count = sum(1 for c in dynamic_constraints if c['First'] == task_id)

        # 4. Task duration
        duration = int(self.tasks[task_id]['duration'])

        # Calculate priority score (lower is higher priority)
        priority = (
            (100 - days_to_delivery) * 10 +           # Urgency factor
            (10000 - critical_path_length) * 5 +      # Critical path factor (inverted)
            (100 - dependent_count) * 3 +             # Dependency factor
            (100 - duration / 10) * 2                 # Duration factor
        )

        return priority

    def check_resource_conflicts(self):
        """Enhanced resource conflict detection that tracks usage over time"""
        conflicts = []

        if not self.task_schedule:
            return conflicts

        # Group tasks by team
        team_tasks = defaultdict(list)
        for task_id, schedule in self.task_schedule.items():
            team_tasks[schedule['team']].append((task_id, schedule))

        # Check each team's resource usage
        for team, tasks in team_tasks.items():
            # Get team capacity
            capacity = self.team_capacity.get(team, 0) or self.quality_team_capacity.get(team, 0)

            # Create timeline of resource usage
            events = []
            for task_id, schedule in tasks:
                events.append((schedule['start_time'], schedule['mechanics_required'], 'start', task_id))
                events.append((schedule['end_time'], -schedule['mechanics_required'], 'end', task_id))

            # Sort events by time
            events.sort(key=lambda x: (x[0], x[1]))

            # Track resource usage over time
            current_usage = 0
            for time, delta, event_type, task_id in events:
                if event_type == 'start':
                    current_usage += delta
                    if current_usage > capacity:
                        conflicts.append({
                            'team': team,
                            'time': time,
                            'usage': current_usage,
                            'capacity': capacity,
                            'task': task_id
                        })
                else:
                    current_usage += delta  # delta is negative for 'end'

        return conflicts

    def calculate_slack_time(self, task_id):
        """Calculate slack time for a task based on delivery date"""
        # Find product line
        product_line = None

        # Check explicit product associations first
        if task_id in self.task_to_product:
            product_line = self.task_to_product[task_id]
        elif task_id in self.tasks:
            product_line = self.tasks[task_id].get('product_line')
        elif task_id in self.quality_inspections:
            primary_task = self.quality_inspections[task_id]['primary_task']
            if primary_task in self.task_to_product:
                product_line = self.task_to_product[primary_task]
            elif primary_task in self.tasks:
                product_line = self.tasks[primary_task].get('product_line')

        if not product_line:
            product, _ = self.parse_product_task_id(task_id)
            product_line = product

        if not product_line:
            return float('inf')

        delivery_date = self.delivery_dates.get(product_line)
        if not delivery_date:
            return float('inf')

        # Calculate latest start time working backwards from delivery
        latest_finish = delivery_date

        # Get cached dynamic constraints
        dynamic_constraints = self.build_dynamic_dependencies()

        # Get all tasks that must follow this one
        all_successors = set()
        stack = [task_id]

        while stack:
            current = stack.pop()

            for constraint in dynamic_constraints:
                if constraint['First'] == current:
                    successor = constraint['Second']
                    if successor not in all_successors:
                        all_successors.add(successor)
                        stack.append(successor)

        # Calculate total duration of successor chain
        total_successor_duration = sum(int(self.tasks[succ]['duration'])
                                      for succ in all_successors if succ in self.tasks)

        # Add buffer for working hours and days
        buffer_days = total_successor_duration / (8 * 60)  # Assuming 8 hour work days
        latest_start = latest_finish - timedelta(days=buffer_days + 2)  # 2 day safety buffer

        # Return slack in hours
        if task_id in self.task_schedule:
            scheduled_start = self.task_schedule[task_id]['start_time']
            slack = (latest_start - scheduled_start).total_seconds() / 3600
            return slack
        else:
            return 0

    def generate_global_priority_list(self, allow_late_delivery=True, silent_mode=False):
        """Generate the final prioritized task list with product-task instance information"""
        # First schedule all tasks
        self.schedule_tasks(allow_late_delivery=allow_late_delivery, silent_mode=silent_mode)

        # Check for resource conflicts
        conflicts = self.check_resource_conflicts()
        if conflicts and not silent_mode:
            print(f"\n[WARNING] Found {len(conflicts)} resource conflicts:")
            for conflict in conflicts[:5]:  # Show first 5
                print(f"  - {conflict['team']} at {conflict['time']}: {conflict['usage']}/{conflict['capacity']} (task {conflict['task']})")

        # Create priority list based on scheduled start times and slack
        priority_data = []

        for task_id, schedule in self.task_schedule.items():
            slack = self.calculate_slack_time(task_id)

            # Get task type from schedule
            task_type = schedule['task_type']

            # Parse product and task number
            product, task_num = self.parse_product_task_id(task_id)

            # Create display name based on task type
            if task_type == 'Quality Inspection':
                primary_task = self.quality_inspections.get(task_id, {}).get('primary_task', task_id)
                _, primary_num = self.parse_product_task_id(primary_task)
                display_name = f"QI for Task {primary_num if primary_num else task_id}"
            elif task_type == 'Late Part':
                display_name = f"Late Part {task_num if task_num else task_id}"
            elif task_type == 'Rework':
                display_name = f"Rework {task_num if task_num else task_id}"
            else:
                display_name = f"Task {task_num if task_num else task_id}"

            priority_data.append({
                'task_id': task_id,
                'task_num': task_num,
                'task_type': task_type,
                'display_name': display_name,
                'product_line': schedule['product_line'],
                'team': schedule['team'],
                'scheduled_start': schedule['start_time'],
                'scheduled_end': schedule['end_time'],
                'duration_minutes': schedule['duration'],
                'mechanics_required': schedule['mechanics_required'],
                'slack_hours': slack,
                'priority_score': self.calculate_task_priority(task_id),
                'shift': schedule['shift']
            })

        # Sort by scheduled start time, then by slack (less slack = higher priority)
        priority_data.sort(key=lambda x: (x['scheduled_start'], x['slack_hours']))

        # Assign global priority rank
        for i, task in enumerate(priority_data, 1):
            task['global_priority'] = i

        self.global_priority_list = priority_data

        return priority_data

    def filter_by_team(self, team_name):
        """Filter the global priority list for a specific team"""
        return [task for task in self.global_priority_list if task['team'] == team_name]

    def get_daily_schedule(self, date, team_name=None):
        """Get schedule for a specific day, optionally filtered by team"""
        target_date = pd.to_datetime(date).date()

        daily_tasks = []
        for task in self.global_priority_list:
            if task['scheduled_start'].date() == target_date:
                if team_name is None or task['team'] == team_name:
                    daily_tasks.append(task)

        return sorted(daily_tasks, key=lambda x: x['scheduled_start'])

    def calculate_lateness_metrics(self):
        """Calculate lateness metrics for each product line"""
        metrics = {}

        # Check if all tasks were scheduled
        scheduled_count = len(self.task_schedule)
        total_tasks = len(self.tasks)

        for product, delivery_date in self.delivery_dates.items():
            product_tasks = [t for t in self.global_priority_list
                           if t['product_line'] == product]

            if product_tasks:
                last_task_end = max(t['scheduled_end'] for t in product_tasks)
                lateness_days = (last_task_end - delivery_date).days

                # Count task types
                task_type_counts = defaultdict(int)
                for task in product_tasks:
                    task_type_counts[task['task_type']] += 1

                # Count unique task numbers
                unique_tasks = set()
                for task in product_tasks:
                    if task.get('task_num'):
                        unique_tasks.add(task['task_num'])

                metrics[product] = {
                    'delivery_date': delivery_date,
                    'projected_completion': last_task_end,
                    'lateness_days': lateness_days,
                    'on_time': lateness_days <= 0,
                    'total_tasks': len(product_tasks),
                    'unique_tasks': len(unique_tasks),
                    'task_breakdown': dict(task_type_counts)
                }
            else:
                # No tasks scheduled for this product
                metrics[product] = {
                    'delivery_date': delivery_date,
                    'projected_completion': None,
                    'lateness_days': 999999,  # Indicate failure
                    'on_time': False,
                    'total_tasks': 0,
                    'unique_tasks': 0,
                    'task_breakdown': {}
                }

        # Add warning if not all tasks scheduled
        if scheduled_count < total_tasks and not self.debug:
            print(f"\n[WARNING] Lateness metrics based on {scheduled_count}/{total_tasks} scheduled task instances")

        return metrics

    def calculate_makespan(self):
        """Calculate the total makespan (schedule duration) in days"""
        if not self.task_schedule:
            return 0

        # Check if all tasks were scheduled
        scheduled_count = len(self.task_schedule)
        total_tasks = len(self.tasks)
        if scheduled_count < total_tasks:
            # Return a very large number to indicate failure
            return 999999

        start_time = min(sched['start_time'] for sched in self.task_schedule.values())
        end_time = max(sched['end_time'] for sched in self.task_schedule.values())

        # Calculate working days between start and end
        current = start_time.date()
        end_date = end_time.date()
        working_days = 0

        while current <= end_date:
            # Check if it's a working day for any product
            is_working = False
            for product in self.product_tasks.keys():
                if self.is_working_day(datetime.combine(current, datetime.min.time()), product):
                    is_working = True
                    break

            if is_working:
                working_days += 1

            current += timedelta(days=1)

        return working_days

    def export_results(self, filename='scheduling_results.csv', scenario_name=''):
        """Export the global priority list to CSV with enhanced product-task instance information"""
        if scenario_name:
            base = 'scheduling_results'
            ext = 'csv'
            if '.' in filename:
                base, ext = filename.rsplit('.', 1)
            filename = f"{base}_{scenario_name}.{ext}"

        if self.global_priority_list:
            df = pd.DataFrame(self.global_priority_list)
            df.to_csv(filename, index=False)
            print(f"Results exported to {filename}")
        else:
            print(f"[WARNING] No tasks to export to {filename}")

        # Also export lateness metrics
        metrics = self.calculate_lateness_metrics()
        if metrics:
            # Prepare metrics for DataFrame
            metrics_data = []
            for product, data in metrics.items():
                row = {
                    'Product Line': product,
                    'Delivery Date': data['delivery_date'],
                    'Projected Completion': data['projected_completion'],
                    'Lateness Days': data['lateness_days'],
                    'On Time': data['on_time'],
                    'Total Task Instances': data['total_tasks'],
                    'Unique Tasks': data.get('unique_tasks', 0)
                }
                # Add task type breakdown
                for task_type, count in data.get('task_breakdown', {}).items():
                    row[f'{task_type} Tasks'] = count
                metrics_data.append(row)

            metrics_df = pd.DataFrame(metrics_data)
            metrics_df.set_index('Product Line', inplace=True)

            if scenario_name:
                metrics_filename = f'lateness_metrics_{scenario_name}.csv'
            else:
                metrics_filename = 'lateness_metrics.csv'

            metrics_df.to_csv(metrics_filename)
            print(f"Lateness metrics exported to {metrics_filename}")
        else:
            print("[WARNING] No lateness metrics to export")

    # ========== SCENARIO 1: Use CSV Headcount ==========
    def scenario_1_csv_headcount(self):
        """
        Scenario 1: Schedule with headcount as defined in CSV, allow late delivery if necessary
        """
        print("\n" + "=" * 80)
        print("SCENARIO 1: Scheduling with CSV-defined Headcount")
        print("=" * 80)

        # Display current capacities from CSV
        print("\nMechanic Team Capacities (from CSV):")
        total_mechanics = 0
        for team, capacity in sorted(self.team_capacity.items()):
            shifts = self.team_shifts.get(team, [])
            total_mechanics += capacity
            print(f"  {team}: {capacity} mechanics (shifts: {', '.join(shifts)})")

        print(f"\nTotal Mechanics: {total_mechanics}")

        print("\nQuality Team Capacities (from CSV):")
        total_quality = 0
        for team, capacity in sorted(self.quality_team_capacity.items()):
            shifts = self.quality_team_shifts.get(team, [])
            total_quality += capacity
            print(f"  {team}: {capacity} quality inspectors (shifts: {', '.join(shifts)})")

        print(f"\nTotal Quality Inspectors: {total_quality}")
        print(f"Total Workforce: {total_mechanics + total_quality}")

        # Generate schedule with allow_late_delivery=True
        priority_list = self.generate_global_priority_list(allow_late_delivery=True)

        # Calculate metrics
        makespan = self.calculate_makespan()
        metrics = self.calculate_lateness_metrics()

        # Display results
        print(f"\nMakespan: {makespan} working days")
        print("\nDelivery Analysis:")
        print("-" * 80)

        total_late_days = 0
        for product, data in metrics.items():
            if data['projected_completion'] is not None:
                status = "ON TIME" if data['on_time'] else f"LATE by {data['lateness_days']} days"
                print(f"{product}: Due {data['delivery_date'].strftime('%Y-%m-%d')}, "
                     f"Projected {data['projected_completion'].strftime('%Y-%m-%d')} - {status}")
                print(f"  Tasks: {data['total_tasks']} instances ({data['unique_tasks']} unique)")
            else:
                print(f"{product}: Due {data['delivery_date'].strftime('%Y-%m-%d')}, "
                     f"Projected UNSCHEDULED - FAILED")
            if data['lateness_days'] > 0 and data['lateness_days'] < 999999:
                total_late_days += data['lateness_days']

        print(f"\nTotal lateness across all products: {total_late_days} days")

        # Export results
        self.export_results(scenario_name='scenario1_csv_capacity')

        return {
            'makespan': makespan,
            'metrics': metrics,
            'total_late_days': total_late_days,
            'priority_list': priority_list,
            'team_capacities': dict(self.team_capacity),
            'quality_capacities': dict(self.quality_team_capacity)
        }

    # ========== SCENARIO 2: Minimize Makespan ==========
    # Replace the existing scenario_2_minimize_makespan method in scheduler.py with this new version

    def scenario_2_just_in_time_optimization(self, min_mechanics=1, max_mechanics=30,
                                             min_quality=1, max_quality=15,
                                             target_lateness=-1, tolerance=2,
                                             max_iterations=300):
        """
        Scenario 2: Find minimum workforce per team to achieve just-in-time delivery

        Target: All products delivered as close to target_lateness (default -1 = 1 day early) as possible
        This provides a buffer while minimizing early completion and resource usage.

        Args:
            min_mechanics: Minimum mechanics to try per team
            max_mechanics: Maximum mechanics to try per team
            min_quality: Minimum quality inspectors to try per team
            max_quality: Maximum quality inspectors to try per team
            target_lateness: Target days late (negative = early). Default -1 = 1 day early
            tolerance: Acceptable deviation from target in days
            max_iterations: Maximum optimization iterations
        """
        print("\n" + "=" * 80)
        print("SCENARIO 2: Just-In-Time Optimization - Minimal Resources for Target Delivery")
        print("=" * 80)
        print(f"Target: All products {abs(target_lateness)} day{'s' if abs(target_lateness) != 1 else ''} early")
        print(f"Tolerance: ±{tolerance} days from target")

        # Save original capacities
        original_team = self._original_team_capacity.copy()
        original_quality = self._original_quality_capacity.copy()

        # Initialize with minimum capacities
        current_config = {
            'mechanic': {team: min_mechanics for team in original_team},
            'quality': {team: min_quality for team in original_quality}
        }

        best_config = None
        best_total_workforce = float('inf')
        best_metrics = None
        best_deviation = float('inf')

        # Phase 1: Find feasible solution with uniform capacity increase
        print("\nPhase 1: Finding initial feasible solution...")
        phase1_complete = False
        uniform_level = min_mechanics

        while uniform_level <= max_mechanics and not phase1_complete:
            # Set uniform capacity
            for team in current_config['mechanic']:
                current_config['mechanic'][team] = uniform_level
            for team in current_config['quality']:
                current_config['quality'][team] = min(uniform_level // 5 + 1, max_quality)

            # Test configuration
            if self._test_configuration_with_target(current_config, target_lateness, tolerance):
                print(
                    f"  Found feasible solution: {uniform_level} mechanics, {min(uniform_level // 5 + 1, max_quality)} quality per team")
                phase1_complete = True
                best_config = {
                    'mechanic': current_config['mechanic'].copy(),
                    'quality': current_config['quality'].copy()
                }
            else:
                uniform_level += 1

        if not phase1_complete:
            print("\n[WARNING] Could not find feasible solution with uniform capacity!")
            print("Trying non-uniform approach...")

            # Start with moderate capacity and adjust
            current_config = {
                'mechanic': {team: (min_mechanics + max_mechanics) // 2 for team in original_team},
                'quality': {team: (min_quality + max_quality) // 2 for team in original_quality}
            }

        # Phase 2: Optimize individual teams to minimize workforce while staying close to target
        print("\nPhase 2: Optimizing individual team capacities...")

        iteration = 0
        no_improvement_count = 0
        max_no_improvement = 30

        while iteration < max_iterations and no_improvement_count < max_no_improvement:
            iteration += 1
            improved = False

            # Apply current configuration and check metrics
            for team, capacity in current_config['mechanic'].items():
                self.team_capacity[team] = capacity
            for team, capacity in current_config['quality'].items():
                self.quality_team_capacity[team] = capacity

            # Schedule and get metrics
            self.task_schedule = {}
            self._critical_path_cache = {}

            try:
                self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

                # Check if all tasks scheduled
                if len(self.task_schedule) < len(self.tasks):
                    # Need more capacity - find bottlenecks
                    bottlenecks = self._identify_bottleneck_teams()
                    for team in bottlenecks['mechanic']:
                        if current_config['mechanic'][team] < max_mechanics:
                            current_config['mechanic'][team] += 1
                            improved = True
                            break
                    if not improved:
                        for team in bottlenecks['quality']:
                            if current_config['quality'][team] < max_quality:
                                current_config['quality'][team] += 1
                                improved = True
                                break
                    continue

                # Calculate metrics
                metrics = self.calculate_lateness_metrics()

                # Calculate deviation from target
                total_deviation = 0
                max_deviation = 0
                within_tolerance = True

                for product, data in metrics.items():
                    lateness = data['lateness_days']
                    if lateness >= 999999:  # Failed to schedule
                        within_tolerance = False
                        max_deviation = 999999
                        break

                    deviation = abs(lateness - target_lateness)
                    total_deviation += deviation
                    max_deviation = max(max_deviation, deviation)

                    # Check if within tolerance
                    if deviation > tolerance:
                        within_tolerance = False

                # Calculate total workforce
                total_workforce = (sum(current_config['mechanic'].values()) +
                                   sum(current_config['quality'].values()))

                if iteration % 10 == 1 or (within_tolerance and total_workforce < best_total_workforce):
                    print(f"  Iteration {iteration}: Workforce = {total_workforce}, "
                          f"Max deviation = {max_deviation:.1f} days, "
                          f"Within tolerance = {within_tolerance}")

                # If within tolerance, try to reduce workforce
                if within_tolerance:
                    if total_workforce < best_total_workforce or max_deviation < best_deviation:
                        best_config = {
                            'mechanic': current_config['mechanic'].copy(),
                            'quality': current_config['quality'].copy()
                        }
                        best_total_workforce = total_workforce
                        best_deviation = max_deviation
                        best_metrics = metrics
                        improved = True

                        print(f"  ✓ New best: {total_workforce} workers, max deviation {max_deviation:.1f} days")

                    # Try reducing underutilized teams
                    utilization = self._calculate_team_utilization()

                    # Find least utilized team
                    min_util = 100
                    min_team = None
                    min_type = None

                    for team, util in utilization['mechanic'].items():
                        if util['utilization'] < min_util and current_config['mechanic'][team] > min_mechanics:
                            min_util = util['utilization']
                            min_team = team
                            min_type = 'mechanic'

                    for team, util in utilization['quality'].items():
                        if util['utilization'] < min_util and current_config['quality'][team] > min_quality:
                            min_util = util['utilization']
                            min_team = team
                            min_type = 'quality'

                    if min_team and min_util < 50:  # If found underutilized team
                        # Try reducing it
                        test_config = {
                            'mechanic': current_config['mechanic'].copy(),
                            'quality': current_config['quality'].copy()
                        }
                        test_config[min_type][min_team] -= 1

                        if self._test_configuration_with_target(test_config, target_lateness, tolerance):
                            current_config = test_config
                            improved = True
                            print(f"    Reduced {min_team} to {test_config[min_type][min_team]} "
                                  f"(utilization was {min_util:.1f}%)")
                else:
                    # Not within tolerance - need to adjust capacity

                    # Find which products are too late or too early
                    adjustments_needed = []
                    for product, data in metrics.items():
                        lateness = data['lateness_days']
                        if lateness >= 999999:
                            continue

                        deviation = lateness - target_lateness
                        if abs(deviation) > tolerance:
                            adjustments_needed.append((product, deviation))

                    if adjustments_needed:
                        # Sort by deviation magnitude
                        adjustments_needed.sort(key=lambda x: abs(x[1]), reverse=True)
                        product, deviation = adjustments_needed[0]

                        if deviation > 0:  # Too late - need more capacity
                            # Find teams used by this product and increase
                            product_tasks = [t for t in self.global_priority_list
                                             if t['product_line'] == product]
                            team_usage = defaultdict(int)
                            for task in product_tasks:
                                team_usage[task['team']] += task['duration_minutes']

                            # Increase most used team
                            if team_usage:
                                most_used_team = max(team_usage.items(), key=lambda x: x[1])[0]
                                if most_used_team in current_config['mechanic']:
                                    if current_config['mechanic'][most_used_team] < max_mechanics:
                                        current_config['mechanic'][most_used_team] += 1
                                        improved = True
                                elif most_used_team in current_config['quality']:
                                    if current_config['quality'][most_used_team] < max_quality:
                                        current_config['quality'][most_used_team] += 1
                                        improved = True
                        else:  # Too early - might reduce capacity (carefully)
                            # Only reduce if we're significantly early
                            if deviation < -(tolerance * 2):
                                # Try reducing least critical team
                                utilization = self._calculate_team_utilization()
                                for team, util in sorted(utilization['mechanic'].items(),
                                                         key=lambda x: x[1]['utilization']):
                                    if current_config['mechanic'][team] > min_mechanics:
                                        test_config = {
                                            'mechanic': current_config['mechanic'].copy(),
                                            'quality': current_config['quality'].copy()
                                        }
                                        test_config['mechanic'][team] -= 1

                                        if self._test_configuration_with_target(test_config, target_lateness,
                                                                                tolerance * 1.5):
                                            current_config = test_config
                                            improved = True
                                            break

                if not improved:
                    no_improvement_count += 1
                else:
                    no_improvement_count = 0

            except Exception as e:
                print(f"  Iteration {iteration}: Scheduling failed - {str(e)}")
                # Increase minimum capacity teams
                min_mech = min(current_config['mechanic'].values())
                for team in current_config['mechanic']:
                    if current_config['mechanic'][team] == min_mech:
                        current_config['mechanic'][team] += 1
                        break

        if best_config is None:
            print("\n[ERROR] Could not find configuration meeting target!")
            # Return with increased capacity
            for team, capacity in original_team.items():
                self.team_capacity[team] = capacity
            for team, capacity in original_quality.items():
                self.quality_team_capacity[team] = capacity
            return None

        print(f"\n✓ Optimization complete after {iteration} iterations")

        # Phase 3: Final verification and results
        print("\nPhase 3: Final verification...")

        # Apply best configuration
        for team, capacity in best_config['mechanic'].items():
            self.team_capacity[team] = capacity
        for team, capacity in best_config['quality'].items():
            self.quality_team_capacity[team] = capacity

        # Generate final schedule
        self.task_schedule = {}
        self._critical_path_cache = {}
        priority_list = self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

        # Calculate final metrics
        makespan = self.calculate_makespan()
        metrics = self.calculate_lateness_metrics()

        # Display results
        print("\n" + "=" * 80)
        print("JUST-IN-TIME OPTIMIZATION RESULTS")
        print("=" * 80)

        print(f"\nTarget Delivery: {abs(target_lateness)} day{'s' if abs(target_lateness) != 1 else ''} early")
        print(f"Achieved Delivery Performance:")

        for product in sorted(metrics.keys()):
            data = metrics[product]
            if data['projected_completion'] is not None:
                lateness = data['lateness_days']
                deviation = abs(lateness - target_lateness)
                status = "✓ ON TARGET" if deviation <= tolerance else "✗ OUTSIDE TOLERANCE"

                if lateness < 0:
                    delivery_desc = f"{abs(lateness)} days early"
                elif lateness > 0:
                    delivery_desc = f"{lateness} days late"
                else:
                    delivery_desc = "exactly on time"

                print(f"  {product}: {delivery_desc} (deviation: {deviation:.1f} days) - {status}")

        print("\nOptimized Mechanic Team Capacities:")
        total_mechanics = 0
        for team in sorted(best_config['mechanic']):
            capacity = best_config['mechanic'][team]
            original = original_team[team]
            total_mechanics += capacity
            diff = capacity - original
            symbol = "↑" if diff > 0 else "↓" if diff < 0 else "="
            print(f"  {team}: {capacity} mechanics (was {original}, {symbol}{abs(diff)})")

        print(f"\nOptimized Quality Team Capacities:")
        total_quality = 0
        for team in sorted(best_config['quality']):
            capacity = best_config['quality'][team]
            original = original_quality[team]
            total_quality += capacity
            diff = capacity - original
            symbol = "↑" if diff > 0 else "↓" if diff < 0 else "="
            print(f"  {team}: {capacity} inspectors (was {original}, {symbol}{abs(diff)})")

        print(f"\nWorkforce Summary:")
        print(f"  Total Mechanics: {total_mechanics} (was {sum(original_team.values())})")
        print(f"  Total Quality: {total_quality} (was {sum(original_quality.values())})")
        print(f"  TOTAL WORKFORCE: {total_mechanics + total_quality} "
              f"(was {sum(original_team.values()) + sum(original_quality.values())})")

        original_total = sum(original_team.values()) + sum(original_quality.values())
        new_total = total_mechanics + total_quality

        if new_total < original_total:
            savings = original_total - new_total
            print(f"  SAVINGS: {savings} workers ({(savings / original_total * 100):.1f}% reduction)")
        elif new_total > original_total:
            increase = new_total - original_total
            print(f"  INCREASE: {increase} workers ({(increase / original_total * 100):.1f}% more)")

        print(f"\nSchedule Metrics:")
        print(f"  Makespan: {makespan} working days")
        print(f"  Maximum deviation from target: {best_deviation:.1f} days")

        # Export results
        self.export_results(scenario_name='scenario2_just_in_time_optimized')

        # Restore original capacities
        for team, capacity in original_team.items():
            self.team_capacity[team] = capacity
        for team, capacity in original_quality.items():
            self.quality_team_capacity[team] = capacity

        return {
            'config': best_config,
            'total_workforce': total_mechanics + total_quality,
            'makespan': makespan,
            'metrics': metrics,
            'target_lateness': target_lateness,
            'max_deviation': best_deviation,
            'priority_list': priority_list
        }

    # Add this helper method to test configurations against target
    def _test_configuration_with_target(self, config, target_lateness, tolerance):
        """Test if a configuration meets the target lateness within tolerance"""
        # Apply configuration
        for team, capacity in config['mechanic'].items():
            self.team_capacity[team] = capacity
        for team, capacity in config['quality'].items():
            self.quality_team_capacity[team] = capacity

        # Clear cache and schedule
        self.task_schedule = {}
        self._critical_path_cache = {}

        try:
            # Generate schedule
            self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

            # Check if all tasks scheduled
            if len(self.task_schedule) < len(self.tasks):
                return False

            # Check lateness metrics
            metrics = self.calculate_lateness_metrics()

            # Check each product
            for product, data in metrics.items():
                lateness = data['lateness_days']
                if lateness >= 999999:  # Failed to schedule
                    return False

                # Check if within tolerance of target
                deviation = abs(lateness - target_lateness)
                if deviation > tolerance:
                    return False

            return True

        except:
            return False

    # ========== SCENARIO 3: Multi-Dimensional Optimization ==========
    def scenario_3_multidimensional_optimization(self, min_mechanics=1, max_mechanics=20,
                                                min_quality=1, max_quality=10,
                                                max_iterations=300):
        """
        Scenario 3 Advanced: Multi-dimensional optimization to find minimum achievable lateness
        and the minimum headcount per team to achieve it.

        This uses a two-phase iterative refinement algorithm:
        Phase 1: Find minimum achievable lateness
        - Starts with minimum capacity
        - Increases bottleneck teams until lateness stops improving
        - Accepts the minimum achievable lateness (may not be zero)

        Phase 2: Optimize workforce while maintaining minimum lateness
        - Reduces capacity for underutilized teams
        - Ensures lateness doesn't increase beyond the minimum found
        """
        print("\n" + "=" * 80)
        print("SCENARIO 3: Multi-Dimensional Team Optimization")
        print("=" * 80)
        print("Finding minimum achievable lateness and optimal capacity per team...")

        # Save original capacities
        original_team = self._original_team_capacity.copy()
        original_quality = self._original_quality_capacity.copy()

        # Initialize with minimum capacities
        current_mech_config = {team: min_mechanics for team in original_team}
        current_qual_config = {team: min_quality for team in original_quality}

        # Track best configuration found
        best_config = None
        best_total_workforce = float('inf')
        best_metrics = None
        best_max_lateness = float('inf')
        best_total_lateness = float('inf')

        # Track if we're still improving
        iterations_without_improvement = 0
        max_iterations_without_improvement = 20

        # Phase 1: Find minimum achievable lateness
        print("\nPhase 1: Finding minimum achievable lateness...")
        iteration = 0
        phase1_complete = False
        previous_max_lateness = float('inf')
        previous_total_lateness = float('inf')

        while iteration < max_iterations and not phase1_complete:
            iteration += 1

            # Apply current configuration
            for team, capacity in current_mech_config.items():
                self.team_capacity[team] = capacity
            for team, capacity in current_qual_config.items():
                self.quality_team_capacity[team] = capacity

            # Clear cache and schedule
            self.task_schedule = {}
            self._critical_path_cache = {}

            try:
                # Generate schedule
                self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

                # Check if all tasks scheduled
                scheduled_count = len(self.task_schedule)
                total_tasks = len(self.tasks)

                if scheduled_count < total_tasks:
                    # Find which team types are blocking
                    unscheduled_tasks = [t for t in self.tasks if t not in self.task_schedule]
                    blocking_teams = self._identify_blocking_teams(unscheduled_tasks)

                    # Increase capacity for blocking teams
                    capacity_increased = False
                    for team in blocking_teams['mechanic']:
                        if current_mech_config[team] < max_mechanics:
                            current_mech_config[team] += 1
                            capacity_increased = True
                            if iteration % 10 == 1:
                                print(f"  Iteration {iteration}: Increased {team} to {current_mech_config[team]} mechanics")

                    for team in blocking_teams['quality']:
                        if current_qual_config[team] < max_quality:
                            current_qual_config[team] += 1
                            capacity_increased = True
                            if iteration % 10 == 1:
                                print(f"  Iteration {iteration}: Increased {team} to {current_qual_config[team]} quality")

                    if not capacity_increased:
                        print(f"\n[WARNING] Cannot increase capacity further. Max limits reached.")
                        print(f"[INFO] Accepting current lateness as minimum achievable.")
                        phase1_complete = True
                    continue

                # Calculate metrics
                metrics = self.calculate_lateness_metrics()
                max_lateness = max((data['lateness_days'] for data in metrics.values()
                                  if data['lateness_days'] < 999999), default=0)
                total_lateness = sum(max(0, data['lateness_days']) for data in metrics.values()
                                   if data['lateness_days'] < 999999)

                # Calculate total workforce
                total_workforce = (sum(current_mech_config.values()) +
                                 sum(current_qual_config.values()))

                # Check if we've improved
                improved = False
                if max_lateness < previous_max_lateness:
                    improved = True
                    previous_max_lateness = max_lateness
                elif max_lateness == previous_max_lateness and total_lateness < previous_total_lateness:
                    improved = True
                    previous_total_lateness = total_lateness

                if iteration % 10 == 1 or improved or max_lateness == 0:
                    print(f"  Iteration {iteration}: Max lateness = {max_lateness} days, "
                         f"Total lateness = {total_lateness} days, "
                         f"Workforce = {total_workforce}")

                # Save if this is the best so far
                if max_lateness < best_max_lateness or (
                    max_lateness == best_max_lateness and total_lateness < best_total_lateness):
                    best_max_lateness = max_lateness
                    best_total_lateness = total_lateness
                    best_config = {
                        'mechanic': current_mech_config.copy(),
                        'quality': current_qual_config.copy()
                    }
                    best_total_workforce = total_workforce
                    best_metrics = metrics
                    iterations_without_improvement = 0

                    if max_lateness == 0:
                        print(f"\n✓ Achieved zero lateness at iteration {iteration}!")
                        phase1_complete = True
                else:
                    iterations_without_improvement += 1

                # Check if we should stop (no improvement for many iterations)
                if iterations_without_improvement >= max_iterations_without_improvement:
                    print(f"\n[INFO] No improvement for {max_iterations_without_improvement} iterations.")
                    print(f"[INFO] Minimum achievable lateness: {best_max_lateness} days")
                    phase1_complete = True
                    continue

                # If not improving, identify bottlenecks and increase their capacity
                if not improved:
                    bottlenecks = self._identify_bottleneck_teams()

                    # Focus on teams causing the most lateness
                    capacity_increased = False

                    # Prioritize mechanic teams first
                    for team in bottlenecks['mechanic']:
                        if current_mech_config[team] < max_mechanics:
                            current_mech_config[team] += 2  # Increase by 2 for faster convergence
                            capacity_increased = True
                            break

                    if not capacity_increased:
                        for team in bottlenecks['quality']:
                            if current_qual_config[team] < max_quality:
                                current_qual_config[team] += 1
                                capacity_increased = True
                                break

                    # If no bottlenecks identified, increase the team with minimum capacity
                    if not capacity_increased:
                        min_mech_cap = min(current_mech_config.values())
                        for team, cap in current_mech_config.items():
                            if cap == min_mech_cap and cap < max_mechanics:
                                current_mech_config[team] += 1
                                capacity_increased = True
                                break

                    if not capacity_increased:
                        min_qual_cap = min(current_qual_config.values())
                        for team, cap in current_qual_config.items():
                            if cap == min_qual_cap and cap < max_quality:
                                current_qual_config[team] += 1
                                capacity_increased = True
                                break

                    if not capacity_increased:
                        print(f"\n[INFO] All teams at maximum capacity.")
                        print(f"[INFO] Minimum achievable lateness: {best_max_lateness} days")
                        phase1_complete = True

            except Exception as e:
                print(f"  Iteration {iteration}: Scheduling failed - {str(e)}")
                # Increase minimum capacity teams
                min_mech = min(current_mech_config.values())
                for team in current_mech_config:
                    if current_mech_config[team] == min_mech and current_mech_config[team] < max_mechanics:
                        current_mech_config[team] += 1
                        break

        if best_config is None:
            print("\n[ERROR] Could not find any feasible solution!")
            # Restore and return
            for team, capacity in original_team.items():
                self.team_capacity[team] = capacity
            for team, capacity in original_quality.items():
                self.quality_team_capacity[team] = capacity
            return None

        print(f"\n✓ Phase 1 Complete!")
        print(f"  Minimum achievable max lateness: {best_max_lateness} days")
        print(f"  Total lateness: {best_total_lateness} days")
        print(f"  Initial workforce: {best_total_workforce}")

        # Phase 2: Optimize by reducing underutilized teams while maintaining minimum lateness
        print("\nPhase 2: Optimizing workforce while maintaining minimum lateness...")

        target_max_lateness = best_max_lateness
        target_total_lateness = best_total_lateness * 1.1  # Allow 10% increase in total for optimization

        improved = True
        optimization_iterations = 0

        while improved and optimization_iterations < 50:
            improved = False
            optimization_iterations += 1

            # Calculate team utilization with current configuration
            for team, capacity in best_config['mechanic'].items():
                self.team_capacity[team] = capacity
            for team, capacity in best_config['quality'].items():
                self.quality_team_capacity[team] = capacity

            # Generate schedule to analyze utilization
            self.task_schedule = {}
            self._critical_path_cache = {}
            self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

            # Calculate utilization for each team
            team_utilization = self._calculate_team_utilization()

            # Try reducing capacity for underutilized mechanic teams
            for team, util_data in sorted(team_utilization['mechanic'].items(),
                                         key=lambda x: x[1]['utilization']):
                if util_data['utilization'] < 0.7 and best_config['mechanic'][team] > min_mechanics:
                    # Try reducing by 1
                    test_config = {
                        'mechanic': best_config['mechanic'].copy(),
                        'quality': best_config['quality'].copy()
                    }
                    test_config['mechanic'][team] -= 1

                    # Test if still maintains minimum lateness
                    if self._test_configuration_with_lateness_target(test_config, target_max_lateness, target_total_lateness):
                        best_config = test_config
                        best_total_workforce -= 1
                        improved = True
                        print(f"  Reduced {team} to {test_config['mechanic'][team]} "
                             f"(utilization was {util_data['utilization']:.1%})")
                        break  # One change at a time

            # Try reducing quality teams if no mechanic reduction worked
            if not improved:
                for team, util_data in sorted(team_utilization['quality'].items(),
                                             key=lambda x: x[1]['utilization']):
                    if util_data['utilization'] < 0.7 and best_config['quality'][team] > min_quality:
                        # Check if this team handles multi-person inspections
                        max_inspectors_needed = util_data.get('max_concurrent', 1)
                        if best_config['quality'][team] > max_inspectors_needed:
                            # Try reducing by 1
                            test_config = {
                                'mechanic': best_config['mechanic'].copy(),
                                'quality': best_config['quality'].copy()
                            }
                            test_config['quality'][team] -= 1

                            # Test if still maintains minimum lateness
                            if self._test_configuration_with_lateness_target(test_config, target_max_lateness, target_total_lateness):
                                best_config = test_config
                                best_total_workforce -= 1
                                improved = True
                                print(f"  Reduced {team} to {test_config['quality'][team]} "
                                     f"(utilization was {util_data['utilization']:.1%})")
                                break

        # Phase 3: Final verification and results
        print("\nPhase 3: Final verification...")

        # Apply best configuration
        for team, capacity in best_config['mechanic'].items():
            self.team_capacity[team] = capacity
        for team, capacity in best_config['quality'].items():
            self.quality_team_capacity[team] = capacity

        # Generate final schedule
        self.task_schedule = {}
        self._critical_path_cache = {}
        priority_list = self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

        # Calculate final metrics
        makespan = self.calculate_makespan()
        metrics = self.calculate_lateness_metrics()

        final_max_lateness = max((data['lateness_days'] for data in metrics.values()
                                if data['lateness_days'] < 999999), default=0)
        final_total_lateness = sum(max(0, data['lateness_days']) for data in metrics.values()
                                 if data['lateness_days'] < 999999)

        # Display results
        print("\n" + "=" * 80)
        print("MULTI-DIMENSIONAL OPTIMIZATION RESULTS")
        print("=" * 80)

        print(f"\nMinimum Achievable Lateness:")
        print(f"  Maximum lateness: {final_max_lateness} days")
        print(f"  Total lateness: {final_total_lateness} days")

        print("\nOptimized Mechanic Team Capacities:")
        total_mechanics = 0
        for team in sorted(best_config['mechanic']):
            capacity = best_config['mechanic'][team]
            original = original_team[team]
            total_mechanics += capacity
            diff = capacity - original
            symbol = "↑" if diff > 0 else "↓" if diff < 0 else "="
            print(f"  {team}: {capacity} mechanics (was {original}, {symbol}{abs(diff)})")

        print(f"\nOptimized Quality Team Capacities:")
        total_quality = 0
        for team in sorted(best_config['quality']):
            capacity = best_config['quality'][team]
            original = original_quality[team]
            total_quality += capacity
            diff = capacity - original
            symbol = "↑" if diff > 0 else "↓" if diff < 0 else "="
            print(f"  {team}: {capacity} inspectors (was {original}, {symbol}{abs(diff)})")

        print(f"\nWorkforce Summary:")
        print(f"  Total Mechanics: {total_mechanics} (was {sum(original_team.values())})")
        print(f"  Total Quality: {total_quality} (was {sum(original_quality.values())})")
        print(f"  TOTAL WORKFORCE: {total_mechanics + total_quality} "
              f"(was {sum(original_team.values()) + sum(original_quality.values())})")

        original_total = sum(original_team.values()) + sum(original_quality.values())
        new_total = total_mechanics + total_quality

        if new_total < original_total:
            savings = original_total - new_total
            print(f"  SAVINGS: {savings} workers ({(savings/original_total*100):.1f}% reduction)")
        elif new_total > original_total:
            increase = new_total - original_total
            print(f"  INCREASE: {increase} workers ({(increase/original_total*100):.1f}% more)")

        print(f"\nSchedule Metrics:")
        print(f"  Makespan: {makespan} working days")

        print("\nDelivery Status by Product:")
        for product in sorted(metrics.keys()):
            data = metrics[product]
            if data['projected_completion'] is not None:
                if data['on_time']:
                    status = "✓ ON TIME"
                    days_info = f"({(data['delivery_date'] - data['projected_completion']).days} days early)"
                else:
                    status = "✗ LATE"
                    days_info = f"({data['lateness_days']} days late)"
                print(f"  {product}: {status} {days_info}")
                print(f"    Due: {data['delivery_date'].strftime('%Y-%m-%d')}, "
                     f"Projected: {data['projected_completion'].strftime('%Y-%m-%d')}")
                print(f"    Tasks: {data['total_tasks']} instances ({data['unique_tasks']} unique)")
            else:
                print(f"  {product}: ✗ UNSCHEDULED")

        # Export results
        self.export_results(scenario_name='scenario3_minimum_lateness_optimized')

        # Restore original capacities
        for team, capacity in original_team.items():
            self.team_capacity[team] = capacity
        for team, capacity in original_quality.items():
            self.quality_team_capacity[team] = capacity

        return {
            'config': best_config,
            'total_workforce': total_mechanics + total_quality,
            'makespan': makespan,
            'metrics': metrics,
            'max_lateness': final_max_lateness,
            'total_lateness': final_total_lateness,
            'priority_list': priority_list
        }

    # ========== Utility methods for optimization scenarios ==========
    def _identify_blocking_teams(self, unscheduled_tasks):
        """Identify which teams are blocking unscheduled tasks"""
        blocking_teams = {'mechanic': set(), 'quality': set()}

        for task_id in unscheduled_tasks:
            if task_id in self.tasks:
                task_info = self.tasks[task_id]
                if task_info['is_quality']:
                    # Find quality teams that could handle this
                    for team in self.quality_team_capacity:
                        blocking_teams['quality'].add(team)
                else:
                    # Add the specific mechanic team
                    blocking_teams['mechanic'].add(task_info['team'])

        return blocking_teams

    def _identify_bottleneck_teams(self):
        """Identify bottleneck teams by analyzing schedule congestion"""
        bottlenecks = {'mechanic': set(), 'quality': set()}

        # Analyze team utilization and queue lengths
        team_load = defaultdict(lambda: {'total_minutes': 0, 'peak_concurrent': 0})

        for task_id, schedule in self.task_schedule.items():
            team = schedule['team']
            duration = schedule['duration']
            mechanics = schedule['mechanics_required']

            team_load[team]['total_minutes'] += duration * mechanics

            # Check concurrent usage at task start
            concurrent = 0
            for other_id, other_schedule in self.task_schedule.items():
                if (other_schedule['team'] == team and
                    other_schedule['start_time'] <= schedule['start_time'] < other_schedule['end_time']):
                    concurrent += other_schedule['mechanics_required']

            team_load[team]['peak_concurrent'] = max(team_load[team]['peak_concurrent'], concurrent)

        # Find teams at or near capacity
        for team, load_data in team_load.items():
            capacity = self.team_capacity.get(team, 0) or self.quality_team_capacity.get(team, 0)
            if load_data['peak_concurrent'] >= capacity * 0.9:
                if team in self.team_capacity:
                    bottlenecks['mechanic'].add(team)
                else:
                    bottlenecks['quality'].add(team)

        return bottlenecks

    def _calculate_team_utilization(self):
        """Calculate detailed utilization metrics for each team"""
        utilization = {'mechanic': {}, 'quality': {}}

        # Working minutes per day per shift
        minutes_per_shift = 8.5 * 60
        total_days = self.calculate_makespan()

        # Calculate for mechanic teams
        for team in self.team_capacity:
            scheduled_minutes = 0
            max_concurrent = 0

            for task_id, schedule in self.task_schedule.items():
                if schedule['team'] == team:
                    scheduled_minutes += schedule['duration'] * schedule['mechanics_required']

                    # Track max concurrent need
                    concurrent_at_start = sum(
                        s['mechanics_required'] for s in self.task_schedule.values()
                        if s['team'] == team and
                        s['start_time'] <= schedule['start_time'] < s['end_time']
                    )
                    max_concurrent = max(max_concurrent, concurrent_at_start)

            capacity = self.team_capacity[team]
            shifts_per_day = len(self.team_shifts.get(team, []))
            available_minutes = capacity * shifts_per_day * minutes_per_shift * total_days

            utilization['mechanic'][team] = {
                'utilization': scheduled_minutes / available_minutes if available_minutes > 0 else 0,
                'scheduled_minutes': scheduled_minutes,
                'available_minutes': available_minutes,
                'max_concurrent': max_concurrent
            }

        # Calculate for quality teams
        for team in self.quality_team_capacity:
            scheduled_minutes = 0
            max_concurrent = 0

            for task_id, schedule in self.task_schedule.items():
                if schedule['team'] == team:
                    scheduled_minutes += schedule['duration'] * schedule['mechanics_required']

                    # Track max concurrent need
                    concurrent_at_start = sum(
                        s['mechanics_required'] for s in self.task_schedule.values()
                        if s['team'] == team and
                        s['start_time'] <= schedule['start_time'] < s['end_time']
                    )
                    max_concurrent = max(max_concurrent, concurrent_at_start)

            capacity = self.quality_team_capacity[team]
            shifts_per_day = len(self.quality_team_shifts.get(team, []))
            available_minutes = capacity * shifts_per_day * minutes_per_shift * total_days

            utilization['quality'][team] = {
                'utilization': scheduled_minutes / available_minutes if available_minutes > 0 else 0,
                'scheduled_minutes': scheduled_minutes,
                'available_minutes': available_minutes,
                'max_concurrent': max_concurrent
            }

        return utilization

    def _test_configuration_with_lateness_target(self, config, target_max_lateness, target_total_lateness):
        """Test if a configuration maintains the target lateness levels"""
        # Apply configuration
        for team, capacity in config['mechanic'].items():
            self.team_capacity[team] = capacity
        for team, capacity in config['quality'].items():
            self.quality_team_capacity[team] = capacity

        # Clear cache and schedule
        self.task_schedule = {}
        self._critical_path_cache = {}

        try:
            # Generate schedule
            self.generate_global_priority_list(allow_late_delivery=True, silent_mode=True)

            # Check if all tasks scheduled
            if len(self.task_schedule) < len(self.tasks):
                return False

            # Check lateness metrics
            metrics = self.calculate_lateness_metrics()
            max_lateness = max((data['lateness_days'] for data in metrics.values()
                              if data['lateness_days'] < 999999), default=0)
            total_lateness = sum(max(0, data['lateness_days']) for data in metrics.values()
                               if data['lateness_days'] < 999999)

            # Must not exceed target max lateness and should be close to target total
            return max_lateness <= target_max_lateness and total_lateness <= target_total_lateness

        except:
            return False

    def simulate_priority_change(self, priority_product, priority_level='high', simulation_days=30):
        """
        Simulate the impact of prioritizing a specific product

        Args:
            priority_product: Product to prioritize
            priority_level: 'high', 'critical', or 'exclusive'
            simulation_days: Days to simulate

        Returns:
            Dictionary with simulation results
        """
        # Save current state
        original_schedule = self.task_schedule.copy()
        original_priorities = self.global_priority_list.copy()

        # Adjust priorities for the selected product
        priority_multipliers = {
            'high': 0.5,      # Reduce priority score by 50% (lower is higher priority)
            'critical': 0.25,  # Reduce by 75%
            'exclusive': 0.1   # Reduce by 90%
        }

        multiplier = priority_multipliers.get(priority_level, 0.5)

        # Re-calculate priorities with bias
        for task in self.global_priority_list:
            if task['product_line'] == priority_product:
                task['priority_score'] *= multiplier

        # Re-sort by new priorities
        self.global_priority_list.sort(key=lambda x: x['priority_score'])

        # Re-schedule with new priorities
        self.schedule_tasks(allow_late_delivery=True, silent_mode=True)

        # Calculate impacts
        new_metrics = self.calculate_lateness_metrics()

        # Restore original state
        self.task_schedule = original_schedule
        self.global_priority_list = original_priorities

        # Return comparison
        return {
            'prioritized_product': priority_product,
            'new_metrics': new_metrics,
            'priority_level': priority_level
        }


# Update the main execution section in scheduler.py (around line 2680)
# Replace the SCENARIO 2 section with this:

if __name__ == "__main__":
    try:
        # Instantiate the scheduler
        scheduler = ProductionScheduler('scheduling_data.csv', debug=True)
        scheduler.load_data_from_csv()

        # Display summary of loaded data
        print("\n" + "=" * 80)
        print("DATA LOADED SUCCESSFULLY")
        print("=" * 80)

        # Count task types
        task_type_counts = defaultdict(int)
        for task_info in scheduler.tasks.values():
            task_type_counts[task_info['task_type']] += 1

        print(f"Total task instances: {len(scheduler.tasks)}")
        for task_type, count in sorted(task_type_counts.items()):
            print(f"- {task_type}: {count}")

        print(f"\nProduct lines: {len(scheduler.delivery_dates)}")
        print(f"Mechanic teams: {len(scheduler.team_capacity)}")
        print(f"Quality teams: {len(scheduler.quality_team_capacity)}")
        print(f"Late part delay: {scheduler.late_part_delay_days} days after on-dock date")

        # Store results
        results = {}

        # BASELINE: Run with original CSV capacities
        print("\n" + "=" * 80)
        print("Running BASELINE scenario with original CSV capacities...")
        print("=" * 80)
        baseline_list = scheduler.generate_global_priority_list()
        results['baseline'] = {
            'makespan': scheduler.calculate_makespan(),
            'metrics': scheduler.calculate_lateness_metrics(),
            'total_workforce': sum(scheduler._original_team_capacity.values()) + sum(
                scheduler._original_quality_capacity.values()),
            'priority_list': baseline_list
        }
        scheduler.export_results(scenario_name='baseline')

        # SCENARIO 1: Use CSV-defined headcount
        print("\n" + "=" * 80)
        print("Running Scenario 1: CSV-defined Capacities...")
        print("=" * 80)
        results['scenario1'] = scheduler.scenario_1_csv_headcount()

        # SCENARIO 2: Just-in-time optimization (target 1 day early)
        print("\n" + "=" * 80)
        print("Running Scenario 2: Just-In-Time Optimization (Target: 1 day early)...")
        print("=" * 80)
        results['scenario2'] = scheduler.scenario_2_just_in_time_optimization(
            min_mechanics=1, max_mechanics=30,
            min_quality=1, max_quality=10,
            target_lateness=-1,  # Target 1 day early
            tolerance=2  # Accept within 2 days of target
        )

        # SCENARIO 3: Multi-dimensional optimization (minimize lateness)
        print("\n" + "=" * 80)
        print("Running Scenario 3: Multi-Dimensional Optimization (Minimize Lateness)...")
        print("=" * 80)
        results['scenario3'] = scheduler.scenario_3_multidimensional_optimization(
            min_mechanics=1, max_mechanics=30,
            min_quality=1, max_quality=15,
            max_iterations=300
        )

        # ========== FINAL SUMMARY ==========
        print("\n" + "=" * 80)
        print("FINAL RESULTS SUMMARY - ALL SCENARIOS")
        print("=" * 80)

        # Display comparison of all scenarios
        print("\nScenario Comparison:")
        print("-" * 100)
        print(f"{'Scenario':<25} {'Workforce':<15} {'Makespan':<15} {'Max Lateness':<15} {'Notes':<30}")
        print("-" * 100)

        for scenario_name, result in results.items():
            if result and 'metrics' in result:
                workforce = result.get('total_workforce', 'N/A')
                makespan = result.get('makespan', 'N/A')
                max_lateness = max((m['lateness_days'] for m in result['metrics'].values()
                                    if m['lateness_days'] < 999999), default='N/A')

                # Add notes based on scenario
                notes = ""
                if scenario_name == 'baseline':
                    notes = "Original CSV capacity"
                elif scenario_name == 'scenario1':
                    notes = "CSV capacity, late OK"
                elif scenario_name == 'scenario2':
                    if result.get('max_deviation'):
                        notes = f"JIT target: 1 day early (±{result.get('max_deviation', 0):.1f}d)"
                    else:
                        notes = "JIT optimization"
                elif scenario_name == 'scenario3':
                    notes = "Minimize lateness"

                print(
                    f"{scenario_name:<25} {str(workforce):<15} {str(makespan):<15} {str(max_lateness):<15} {notes:<30}")

        print("\n" + "=" * 80)
        print("ALL SCENARIOS COMPLETED SUCCESSFULLY!")
        print("=" * 80)

    except Exception as e:
        print("\n" + "!" * 80)
        print(f"ERROR: {str(e)}")
        print("!" * 80)
        import traceback

        traceback.print_exc()