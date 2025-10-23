import pandas as pd
import os
from pathlib import Path
from datetime import datetime

class DRPlanAnalyzer:
    def __init__(self, before_file_path=None, after_file_path=None):
        self.before_file_path = before_file_path
        self.after_file_path = after_file_path
        self.before_data = None
        self.after_data = None
        self.column_mapping = {
            'name': ['Name', 'name'],
            'serial_number': ['Serial Number', 'u_serial_number'],
            'manual_entry': ['Manual Entry', 'u_manual_entry'],
            'type': ['Type', 'u_type'],
            'plan': ['Plan', 'plan'],
            'environment': ['Environment', 'u_environment', 'Server Environment', 'u_server_environment'],
            'dr_device': ['DR Device', 'u_dr_device'],
            'global_load_balancer': ['Global Load Balancer', 'u_global_load_balancer'],
            'nas': ['NAS', 'u_nas'],
            'comments': ['Comments', 'u_comments'],
            'failover_strategy': ['Failover Strategy', 'u_failover_strategy'],
            'plan_invalid': ['Plan Invalid', 'plan.u_plan_invalid']
        }
        self.critical_attributes = ['dr_device', 'global_load_balancer', 'nas', 'comments', 'failover_strategy']
        
    def load_data(self, file_path):
        """Load data from Excel or CSV file"""
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext in ['.xlsx', '.xls']:
            # Load Excel file with all sheets
            excel_file = pd.ExcelFile(file_path)
            dfs = []
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                df['_source_sheet'] = sheet_name
                dfs.append(df)
            data = pd.concat(dfs, ignore_index=True)
            print(f"Loaded {len(data)} records from {len(excel_file.sheet_names)} Excel sheets")
            
        elif file_ext == '.csv':
            # Try common encodings for CSV files
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            loaded = False
            
            for encoding in encodings:
                try:
                    data = pd.read_csv(file_path, encoding=encoding)
                    data['_source_sheet'] = 'CSV'
                    print(f"Loaded {len(data)} records from CSV using {encoding} encoding")
                    loaded = True
                    break
                except (UnicodeDecodeError, Exception) as e:
                    continue
            
            if not loaded:
                # Last resort: try with error handling
                data = pd.read_csv(file_path, encoding='utf-8', errors='ignore')
                data['_source_sheet'] = 'CSV'
                print(f"Loaded {len(data)} records from CSV with error handling")
        else:
            raise ValueError("Unsupported file format. Please provide .xlsx, .xls, or .csv file")
        
        return data
    
    def normalize_columns(self, data):
        """Normalize column names based on mapping"""
        normalized_cols = {}
        
        for standard_name, possible_names in self.column_mapping.items():
            for col in data.columns:
                if col in possible_names:
                    normalized_cols[col] = standard_name
                    break
        
        data.rename(columns=normalized_cols, inplace=True)
        print(f"Normalized columns: {list(normalized_cols.values())}")
        
        # Ensure required columns exist
        required = ['name', 'type', 'plan']
        missing = [col for col in required if col not in data.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        return data
    
    def extract_device_info(self, data):
        """Extract device type and actual name from the name field"""
        data['name_type_prefix'] = None
        data['actual_device_name'] = data['name']
        
        # Extract type prefix from name if colon exists
        mask = data['name'].astype(str).str.contains(':', na=False)
        data.loc[mask, 'name_type_prefix'] = data.loc[mask, 'name'].str.split(':', n=1).str[0].str.strip()
        data.loc[mask, 'actual_device_name'] = data.loc[mask, 'name'].str.split(':', n=1).str[1].str.strip()
        
        return data
    
    def analyze_before_duplicates(self):
        """Analysis 1: Identify duplicate devices in BEFORE file"""
        print("\n=== Analysis 1: Name/Type Duplicates (BEFORE FIX) ===")
        
        if self.before_data is None:
            print("Warning: Before data not loaded. Skipping analysis.")
            return pd.DataFrame()
        
        results = []
        
        for plan in self.before_data['plan'].unique():
            plan_data = self.before_data[self.before_data['plan'] == plan].copy()
            
            # Get plan invalid status
            plan_invalid = 'Unknown'
            if 'plan_invalid' in plan_data.columns:
                plan_invalid_vals = plan_data['plan_invalid'].dropna().unique()
                plan_invalid = plan_invalid_vals[0] if len(plan_invalid_vals) > 0 else 'Unknown'
            
            # Group by actual device name and type
            grouped = plan_data.groupby(['actual_device_name', 'type'])
            
            for (device_name, device_type), group in grouped:
                if len(group) > 1:
                    # Check for mismatches
                    unique_prefixes = group['name_type_prefix'].nunique()
                    unique_full_names = group['name'].nunique()
                    
                    # Determine duplicate type
                    if unique_full_names == 1:
                        duplicate_type = "Exact Duplicate"
                        duplicate_description = "Same full name, same type (true duplicate)"
                    elif unique_prefixes > 1:
                        duplicate_type = "Mismatch Duplicate"
                        duplicate_description = "Different name prefixes but same actual name/type"
                    else:
                        duplicate_type = "Other Duplicate"
                        duplicate_description = "Different full names but same actual name/type"
                    
                    # Check if any are manual entries
                    manual_entries = []
                    if 'manual_entry' in self.before_data.columns:
                        manual_entries = group[group['manual_entry'].astype(str).str.upper().isin(['TRUE', 'T', '1', 'YES'])]['name'].tolist()
                    
                    for idx, row in group.iterrows():
                        # Check for type mismatch between prefix and actual type
                        type_mismatch = 'No'
                        if pd.notna(row.get('name_type_prefix')):
                            prefix_upper = str(row.get('name_type_prefix')).upper()
                            type_upper = str(device_type).upper()
                            if prefix_upper not in type_upper and not any(word in type_upper for word in prefix_upper.split()):
                                type_mismatch = 'Yes'
                        
                        # Check if critical attributes are populated
                        has_attributes = 'No'
                        populated_attrs = []
                        for attr in self.critical_attributes:
                            if attr in row.index and pd.notna(row.get(attr)) and str(row.get(attr)).strip() != '':
                                populated_attrs.append(attr)
                        
                        if populated_attrs:
                            has_attributes = 'Yes'
                        
                        results.append({
                            'Plan': plan,
                            'Plan Invalid': plan_invalid,
                            'Device Name': device_name,
                            'Type': device_type,
                            'Full Name (with prefix)': row['name'],
                            'Name Type Prefix': row.get('name_type_prefix', ''),
                            'Serial Number': row.get('serial_number', ''),
                            'Manual Entry': row.get('manual_entry', ''),
                            'Environment': row.get('environment', ''),
                            'Duplicate Count': len(group),
                            'Duplicate Type': duplicate_type,
                            'Duplicate Description': duplicate_description,
                            'Is Manual Entry': 'Yes' if row['name'] in manual_entries else 'No',
                            'Type Mismatch (Prefix vs Type)': type_mismatch,
                            'Has Critical Attributes': has_attributes,
                            'Populated Attributes': ', '.join(populated_attrs) if populated_attrs else 'None',
                            'DR Device': row.get('dr_device', ''),
                            'Global Load Balancer': row.get('global_load_balancer', ''),
                            'NAS': row.get('nas', ''),
                            'Comments': row.get('comments', ''),
                            'Failover Strategy': row.get('failover_strategy', '')
                        })
        
        df_results = pd.DataFrame(results)
        
        # Print summary stats
        if not df_results.empty:
            print(f"\nTotal duplicate records: {len(df_results)}")
            print(f"\nBreakdown by Duplicate Type:")
            type_counts = df_results.groupby('Duplicate Type').size()
            for dup_type, count in type_counts.items():
                print(f"  - {dup_type}: {count} records")
            
            # Show duplicates with critical attributes
            with_attrs = df_results[df_results['Has Critical Attributes'] == 'Yes']
            print(f"\nDuplicates with critical attributes populated: {len(with_attrs)}")
        
        return df_results
    
    def analyze_attribute_transfer_risk(self):
        """Analysis 2: Identify which duplicates will lose attributes when removed"""
        print("\n=== Analysis 2: Attribute Transfer Risk Analysis ===")
        
        if self.before_data is None:
            print("Warning: Before data not loaded. Skipping analysis.")
            return pd.DataFrame()
        
        results = []
        
        for plan in self.before_data['plan'].unique():
            plan_data = self.before_data[self.before_data['plan'] == plan].copy()
            
            # Get plan invalid status
            plan_invalid = 'Unknown'
            if 'plan_invalid' in plan_data.columns:
                plan_invalid_vals = plan_data['plan_invalid'].dropna().unique()
                plan_invalid = plan_invalid_vals[0] if len(plan_invalid_vals) > 0 else 'Unknown'
            
            # Group by actual device name and type
            grouped = plan_data.groupby(['actual_device_name', 'type'])
            
            for (device_name, device_type), group in grouped:
                if len(group) > 1:
                    # Identify correct CI (where name prefix matches type)
                    correct_ci = None
                    mismatch_cis = []
                    
                    for idx, row in group.iterrows():
                        prefix_upper = str(row.get('name_type_prefix', '')).upper()
                        type_upper = str(device_type).upper()
                        
                        # Check if prefix matches type
                        is_match = prefix_upper in type_upper or any(word in type_upper for word in prefix_upper.split())
                        
                        if is_match:
                            if correct_ci is None:
                                correct_ci = row
                        else:
                            mismatch_cis.append(row)
                    
                    # If no clear correct CI found, use first one
                    if correct_ci is None and len(group) > 0:
                        correct_ci = group.iloc[0]
                        mismatch_cis = [group.iloc[i] for i in range(1, len(group))]
                    
                    # Analyze each mismatch CI
                    for mismatch_ci in mismatch_cis:
                        transfer_needed = []
                        transfer_conflict = []
                        
                        for attr in self.critical_attributes:
                            mismatch_val = mismatch_ci.get(attr, '')
                            correct_val = correct_ci.get(attr, '') if correct_ci is not None else ''
                            
                            # Check if mismatch has value but correct doesn't
                            mismatch_has_val = pd.notna(mismatch_val) and str(mismatch_val).strip() != ''
                            correct_has_val = pd.notna(correct_val) and str(correct_val).strip() != ''
                            
                            if mismatch_has_val and not correct_has_val:
                                transfer_needed.append(f"{attr}: '{mismatch_val}'")
                            elif mismatch_has_val and correct_has_val and str(mismatch_val) != str(correct_val):
                                transfer_conflict.append(f"{attr}: Mismatch='{mismatch_val}' vs Correct='{correct_val}'")
                        
                        risk_level = 'None'
                        if transfer_needed and not transfer_conflict:
                            risk_level = 'Transfer Needed'
                        elif transfer_conflict:
                            risk_level = 'Conflict - Data Loss'
                        elif transfer_needed and transfer_conflict:
                            risk_level = 'Partial Transfer with Conflict'
                        
                        if transfer_needed or transfer_conflict:
                            results.append({
                                'Plan': plan,
                                'Plan Invalid': plan_invalid,
                                'Device Name': device_name,
                                'Type': device_type,
                                'Mismatch CI Name': mismatch_ci['name'],
                                'Correct CI Name': correct_ci['name'] if correct_ci is not None else 'N/A',
                                'Risk Level': risk_level,
                                'Attributes to Transfer': '; '.join(transfer_needed) if transfer_needed else 'None',
                                'Attribute Conflicts': '; '.join(transfer_conflict) if transfer_conflict else 'None',
                                'Action Required': 'Transfer attributes before deletion' if transfer_needed else 'Review conflicts',
                                'Mismatch Serial': mismatch_ci.get('serial_number', ''),
                                'Correct Serial': correct_ci.get('serial_number', '') if correct_ci is not None else ''
                            })
        
        df_results = pd.DataFrame(results)
        
        if not df_results.empty:
            print(f"\nTotal CIs at risk of attribute loss: {len(df_results)}")
            print(f"\nBreakdown by Risk Level:")
            risk_counts = df_results.groupby('Risk Level').size()
            for risk, count in risk_counts.items():
                print(f"  - {risk}: {count} CIs")
        
        return df_results
    
    def analyze_after_duplicates(self):
        """Analysis 3: Identify remaining duplicates in AFTER file"""
        print("\n=== Analysis 3: Name/Type Duplicates (AFTER FIX) ===")
        
        if self.after_data is None:
            print("Warning: After data not loaded. Skipping analysis.")
            return pd.DataFrame()
        
        results = []
        
        for plan in self.after_data['plan'].unique():
            plan_data = self.after_data[self.after_data['plan'] == plan].copy()
            
            # Get plan invalid status
            plan_invalid = 'Unknown'
            if 'plan_invalid' in plan_data.columns:
                plan_invalid_vals = plan_data['plan_invalid'].dropna().unique()
                plan_invalid = plan_invalid_vals[0] if len(plan_invalid_vals) > 0 else 'Unknown'
            
            # Group by actual device name and type
            grouped = plan_data.groupby(['actual_device_name', 'type'])
            
            for (device_name, device_type), group in grouped:
                if len(group) > 1:
                    unique_prefixes = group['name_type_prefix'].nunique()
                    unique_full_names = group['name'].nunique()
                    
                    if unique_full_names == 1:
                        duplicate_type = "Exact Duplicate"
                    elif unique_prefixes > 1:
                        duplicate_type = "Mismatch Duplicate"
                    else:
                        duplicate_type = "Other Duplicate"
                    
                    manual_entries = []
                    if 'manual_entry' in self.after_data.columns:
                        manual_entries = group[group['manual_entry'].astype(str).str.upper().isin(['TRUE', 'T', '1', 'YES'])]['name'].tolist()
                    
                    for idx, row in group.iterrows():
                        type_mismatch = 'No'
                        if pd.notna(row.get('name_type_prefix')):
                            prefix_upper = str(row.get('name_type_prefix')).upper()
                            type_upper = str(device_type).upper()
                            if prefix_upper not in type_upper and not any(word in type_upper for word in prefix_upper.split()):
                                type_mismatch = 'Yes'
                        
                        results.append({
                            'Plan': plan,
                            'Plan Invalid': plan_invalid,
                            'Device Name': device_name,
                            'Type': device_type,
                            'Full Name (with prefix)': row['name'],
                            'Name Type Prefix': row.get('name_type_prefix', ''),
                            'Serial Number': row.get('serial_number', ''),
                            'Duplicate Count': len(group),
                            'Duplicate Type': duplicate_type,
                            'Is Manual Entry': 'Yes' if row['name'] in manual_entries else 'No',
                            'Type Mismatch (Prefix vs Type)': type_mismatch,
                            'Issue': 'Duplicate still exists after fix - needs investigation'
                        })
        
        df_results = pd.DataFrame(results)
        
        if not df_results.empty:
            print(f"\n⚠️  WARNING: {len(df_results)} duplicate records still exist after fix!")
            print(f"\nBreakdown by Duplicate Type:")
            type_counts = df_results.groupby('Duplicate Type').size()
            for dup_type, count in type_counts.items():
                print(f"  - {dup_type}: {count} records")
        else:
            print(f"\n✅ No duplicates found after fix!")
        
        return df_results
    
    def compare_before_after(self):
        """Analysis 4: Compare before and after files"""
        print("\n=== Analysis 4: Before vs After Comparison ===")
        
        if self.before_data is None or self.after_data is None:
            print("Warning: Both before and after data required. Skipping analysis.")
            return pd.DataFrame()
        
        results = []
        
        # Create unique identifiers for comparison
        self.before_data['_uid'] = (self.before_data['plan'].astype(str) + '||' + 
                                     self.before_data['name'].astype(str) + '||' + 
                                     self.before_data['type'].astype(str))
        self.after_data['_uid'] = (self.after_data['plan'].astype(str) + '||' + 
                                    self.after_data['name'].astype(str) + '||' + 
                                    self.after_data['type'].astype(str))
        
        before_uids = set(self.before_data['_uid'].unique())
        after_uids = set(self.after_data['_uid'].unique())
        
        # Find removed CIs
        removed_uids = before_uids - after_uids
        
        for uid in removed_uids:
            removed_ci = self.before_data[self.before_data['_uid'] == uid].iloc[0]
            
            # Check if it had critical attributes
            had_attributes = []
            for attr in self.critical_attributes:
                val = removed_ci.get(attr, '')
                if pd.notna(val) and str(val).strip() != '':
                    had_attributes.append(f"{attr}='{val}'")
            
            results.append({
                'Plan': removed_ci.get('plan', ''),
                'Removed CI Name': removed_ci['name'],
                'Type': removed_ci.get('type', ''),
                'Serial Number': removed_ci.get('serial_number', ''),
                'Was Manual Entry': removed_ci.get('manual_entry', ''),
                'Had Critical Attributes': 'Yes' if had_attributes else 'No',
                'Attributes Lost': '; '.join(had_attributes) if had_attributes else 'None',
                'Status': 'Removed by fix script'
            })
        
        df_results = pd.DataFrame(results)
        
        print(f"\nTotal CIs removed: {len(df_results)}")
        if not df_results.empty:
            with_attrs = df_results[df_results['Had Critical Attributes'] == 'Yes']
            print(f"CIs removed with critical attributes: {len(with_attrs)}")
            if len(with_attrs) > 0:
                print(f"⚠️  WARNING: {len(with_attrs)} CIs with attributes were removed!")
        
        return df_results
    
    def analyze_serial_duplicates_before(self):
        """Analysis 5: Serial/Type duplicates in BEFORE file"""
        print("\n=== Analysis 5: Serial Number/Type Duplicates (BEFORE) ===")
        
        if self.before_data is None or 'serial_number' not in self.before_data.columns:
            print("Warning: Before data or serial number not available. Skipping analysis.")
            return pd.DataFrame()
        
        results = []
        data_with_serial = self.before_data[self.before_data['serial_number'].notna() & (self.before_data['serial_number'] != '')].copy()
        
        for plan in data_with_serial['plan'].unique():
            plan_data = data_with_serial[data_with_serial['plan'] == plan].copy()
            
            plan_invalid = 'Unknown'
            if 'plan_invalid' in plan_data.columns:
                plan_invalid_vals = plan_data['plan_invalid'].dropna().unique()
                plan_invalid = plan_invalid_vals[0] if len(plan_invalid_vals) > 0 else 'Unknown'
            
            grouped = plan_data.groupby(['serial_number', 'type'])
            
            for (serial, device_type), group in grouped:
                if len(group) > 1:
                    unique_full_names = group['name'].nunique()
                    unique_device_names = group['actual_device_name'].nunique()
                    
                    if unique_full_names == 1:
                        duplicate_type = "Exact Duplicate"
                        duplicate_description = "Same serial, same type, same name (true duplicate)"
                    elif unique_device_names == 1:
                        duplicate_type = "Mismatch Duplicate"
                        duplicate_description = "Same serial/type, different name prefixes"
                    else:
                        duplicate_type = "Other Duplicate"
                        duplicate_description = "Same serial/type, different device names"
                    
                    manual_entries = []
                    if 'manual_entry' in self.before_data.columns:
                        manual_entries = group[group['manual_entry'].astype(str).str.upper().isin(['TRUE', 'T', '1', 'YES'])]['name'].tolist()
                    
                    for idx, row in group.iterrows():
                        results.append({
                            'Plan': plan,
                            'Plan Invalid': plan_invalid,
                            'Serial Number': serial,
                            'Type': device_type,
                            'Device Name': row['name'],
                            'Actual Device Name': row.get('actual_device_name', ''),
                            'Name Type Prefix': row.get('name_type_prefix', ''),
                            'Manual Entry': row.get('manual_entry', ''),
                            'Environment': row.get('environment', ''),
                            'Duplicate Count': len(group),
                            'Duplicate Type': duplicate_type,
                            'Duplicate Description': duplicate_description,
                            'Is Manual Entry': 'Yes' if row['name'] in manual_entries else 'No'
                        })
        
        df_results = pd.DataFrame(results)
        
        if not df_results.empty:
            print(f"\nTotal duplicate records: {len(df_results)}")
            print(f"\nBreakdown by Duplicate Type:")
            type_counts = df_results.groupby('Duplicate Type').size()
            for dup_type, count in type_counts.items():
                print(f"  - {dup_type}: {count} records")
        
        return df_results
    
    def analyze_manual_non_production(self):
        """Analysis 6: Manual entries in non-production"""
        print("\n=== Analysis 6: Manual Entry Non-Production Devices ===")
        
        data_to_analyze = self.before_data if self.before_data is not None else self.after_data
        
        if data_to_analyze is None or 'manual_entry' not in data_to_analyze.columns:
            print("Warning: Data or manual entry column not found. Skipping analysis.")
            return pd.DataFrame()
        
        results = []
        manual_data = data_to_analyze[
            data_to_analyze['manual_entry'].astype(str).str.upper().isin(['TRUE', 'T', '1', 'YES'])
        ].copy()
        
        if 'environment' in data_to_analyze.columns:
            non_prod_manual = manual_data[
                ~manual_data['environment'].astype(str).str.upper().str.contains('PROD', na=False)
            ].copy()
        else:
            non_prod_manual = manual_data.copy()
            print("Warning: Environment column not found. Showing all manual entries.")
        
        for idx, row in non_prod_manual.iterrows():
            plan_invalid = row.get('plan_invalid', 'Unknown')
            
            results.append({
                'Plan': row.get('plan', ''),
                'Plan Invalid': plan_invalid,
                'Device Name': row['name'],
                'Actual Device Name': row.get('actual_device_name', ''),
                'Type': row.get('type', ''),
                'Serial Number': row.get('serial_number', ''),
                'Manual Entry': row.get('manual_entry', ''),
                'Environment': row.get('environment', ''),
                'Issue': 'Manual entry in non-production environment on master plan'
            })
        
        return pd.DataFrame(results)
    
    def run_all_analyses(self):
        """Run all analyses and save to Excel"""
        print(f"\n{'='*60}")
        print("DR MASTER PLAN CI DUPLICATE ANALYSIS")
        print(f"{'='*60}")
        
        # Load and process data
        if self.before_file_path:
            print(f"\n📂 Loading BEFORE file: {self.before_file_path}")
            self.before_data = self.load_data(self.before_file_path)
            self.before_data = self.normalize_columns(self.before_data)
            self.before_data = self.extract_device_info(self.before_data)
            print(f"Total records (BEFORE): {len(self.before_data)}")
            print(f"Unique plans (BEFORE): {self.before_data['plan'].nunique()}")
        
        if self.after_file_path:
            print(f"\n📂 Loading AFTER file: {self.after_file_path}")
            self.after_data = self.load_data(self.after_file_path)
            self.after_data = self.normalize_columns(self.after_data)
            self.after_data = self.extract_device_info(self.after_data)
            print(f"Total records (AFTER): {len(self.after_data)}")
            print(f"Unique plans (AFTER): {self.after_data['plan'].nunique()}")
        
        # Run all analyses
        df_before_dups = self.analyze_before_duplicates()
        df_attr_risk = self.analyze_attribute_transfer_risk()
        df_after_dups = self.analyze_after_duplicates()
        df_comparison = self.compare_before_after()
        df_serial_dups = self.analyze_serial_duplicates_before()
        df_manual_nonprod = self.analyze_manual_non_production()
        
        # Generate output file path
        if self.before_file_path:
            input_path = Path(self.before_file_path)
        else:
            input_path = Path(self.after_file_path)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = input_path.parent / f"DR_Plan_Analysis_BeforeAfter_{timestamp}.xlsx"
        
        # Write to Excel
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Summary sheet
            summary_data = {
                'Analysis': [
                    'Analysis 1: BEFORE - Name/Type Duplicates',
                    'Analysis 2: Attribute Transfer Risk',
                    'Analysis 3: AFTER - Name/Type Duplicates',
                    'Analysis 4: Before vs After Comparison',
                    'Analysis 5: BEFORE - Serial/Type Duplicates',
                    'Analysis 6: Manual Non-Prod Entries'
                ],
                'Record Count': [
                    len(df_before_dups),
                    len(df_attr_risk),
                    len(df_after_dups),
                    len(df_comparison),
                    len(df_serial_dups),
                    len(df_manual_nonprod)
                ],
                'Sheet Name': [
                    '1_Before_NameType_Dups',
                    '2_Attribute_Transfer_Risk',
                    '3_After_NameType_Dups',
                    '4_Before_After_Compare',
                    '5_Before_SerialType_Dups',
                    '6_Manual_NonProd'
                ],
                'Description': [
                    'Duplicates found in BEFORE file',
                    'CIs at risk of losing attributes when removed',
                    'Duplicates still present in AFTER file (should be 0)',
                    'CIs removed by fix script',
                    'Serial number duplicates in BEFORE file',
                    'Manual entries in non-production environments'
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Write each analysis
            if not df_before_dups.empty:
                df_before_dups.to_excel(writer, sheet_name='1_Before_NameType_Dups', index=False)
            
            if not df_attr_risk.empty:
                df_attr_risk.to_excel(writer, sheet_name='2_Attribute_Transfer_Risk', index=False)
            
            if not df_after_dups.empty:
                df_after_dups.to_excel(writer, sheet_name='3_After_NameType_Dups', index=False)
            
            if not df_comparison.empty:
                df_comparison.to_excel(writer, sheet_name='4_Before_After_Compare', index=False)
            
            if not df_serial_dups.empty:
                df_serial_dups.to_excel(writer, sheet_name='5_Before_SerialType_Dups', index=False)
            
            if not df_manual_nonprod.empty:
                df_manual_nonprod.to_excel(writer, sheet_name='6_Manual_NonProd', index=False)
        
        print(f"\n{'='*60}")
        print(f"Analysis complete! Results saved to:")
        print(f"{output_file}")
        print(f"{'='*60}\n")
        
        # Print summary
        print("SUMMARY OF FINDINGS:")
        
        if self.before_file_path:
            print(f"\n  Analysis 1 - BEFORE Duplicates: {len(df_before_dups)} duplicate records")
            if not df_before_dups.empty:
                for dup_type, count in df_before_dups['Duplicate Type'].value_counts().items():
                    print(f"    • {dup_type}: {count} records")
            
            print(f"\n  Analysis 2 - Attribute Transfer Risk: {len(df_attr_risk)} CIs at risk")
            if not df_attr_risk.empty:
                for risk, count in df_attr_risk['Risk Level'].value_counts().items():
                    print(f"    • {risk}: {count} CIs")
            
            print(f"\n  Analysis 5 - BEFORE Serial Duplicates: {len(df_serial_dups)} duplicate records")
            if not df_serial_dups.empty:
                for dup_type, count in df_serial_dups['Duplicate Type'].value_counts().items():
                    print(f"    • {dup_type}: {count} records")
        
        if self.after_file_path:
            print(f"\n  Analysis 3 - AFTER Duplicates: {len(df_after_dups)} duplicate records")
            if not df_after_dups.empty:
                print(f"    ⚠️  WARNING: Duplicates still exist after fix!")
                for dup_type, count in df_after_dups['Duplicate Type'].value_counts().items():
                    print(f"    • {dup_type}: {count} records")
            else:
                print(f"    ✅ No duplicates remain after fix!")
        
        if self.before_file_path and self.after_file_path:
            print(f"\n  Analysis 4 - CIs Removed: {len(df_comparison)} CIs")
            if not df_comparison.empty:
                with_attrs = df_comparison[df_comparison['Had Critical Attributes'] == 'Yes']
                print(f"    • CIs with attributes removed: {len(with_attrs)}")
                if len(with_attrs) > 0:
                    print(f"    ⚠️  WARNING: {len(with_attrs)} CIs with critical attributes were removed!")
        
        print(f"\n  Analysis 6 - Manual Non-Prod: {len(df_manual_nonprod)} entries")


def main():
    print("="*60)
    print("DR Master Plan CI Duplicate Analysis Tool")
    print("Before/After Fix Comparison")
    print("="*60)
    
    print("\nThis tool can analyze:")
    print("  1. Only BEFORE file (pre-fix analysis)")
    print("  2. Only AFTER file (post-fix validation)")
    print("  3. Both BEFORE and AFTER files (full comparison)")
    
    # Option 1: Hardcode file paths (uncomment and modify)
    # before_file_path = r"C:\path\to\before_fix.xlsx"
    # after_file_path = r"C:\path\to\after_fix.xlsx"
    
    # Option 2: Interactive input
    print("\n" + "="*60)
    before_file_path = input("Enter path to BEFORE file (or press Enter to skip): ").strip().strip('"')
    if before_file_path and not os.path.exists(before_file_path):
        print(f"⚠️  Warning: BEFORE file not found at {before_file_path}")
        before_file_path = None
    
    after_file_path = input("Enter path to AFTER file (or press Enter to skip): ").strip().strip('"')
    if after_file_path and not os.path.exists(after_file_path):
        print(f"⚠️  Warning: AFTER file not found at {after_file_path}")
        after_file_path = None
    
    if not before_file_path and not after_file_path:
        print("\n❌ Error: At least one file must be provided!")
        return
    
    try:
        # Initialize analyzer
        analyzer = DRPlanAnalyzer(before_file_path, after_file_path)
        
        # Run all analyses
        analyzer.run_all_analyses()
        
        print("\n" + "="*60)
        print("✅ ANALYSIS COMPLETE!")
        print("="*60)
        print("\nKey Insights:")
        print("  • Analysis 1: Shows all duplicates before fix")
        print("  • Analysis 2: Identifies CIs that will lose attributes")
        print("  • Analysis 3: Validates fix worked (should show 0 duplicates)")
        print("  • Analysis 4: Shows what was removed by the fix")
        print("  • Analysis 5: Serial number based duplicates")
        print("  • Analysis 6: Manual non-production entries to review")
        
    except Exception as e:
        print(f"\n❌ Error occurred during analysis: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
