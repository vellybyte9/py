import pandas as pd
import os
from pathlib import Path
from datetime import datetime

class DRPlanAnalyzer:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        self.column_mapping = {
            'name': ['Name', 'name'],
            'serial_number': ['Serial Number', 'u_serial_number'],
            'manual_entry': ['Manual Entry', 'u_manual_entry'],
            'type': ['Type', 'u_type'],
            'plan': ['Plan', 'plan'],
            'environment': ['Environment', 'u_environment', 'Server Environment', 'u_server_environment']
        }
        
    def load_data(self):
        """Load data from Excel or CSV file"""
        file_ext = Path(self.file_path).suffix.lower()
        
        if file_ext in ['.xlsx', '.xls']:
            # Load Excel file with all sheets
            excel_file = pd.ExcelFile(self.file_path)
            dfs = []
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                df['_source_sheet'] = sheet_name
                dfs.append(df)
            self.data = pd.concat(dfs, ignore_index=True)
            print(f"Loaded {len(self.data)} records from {len(excel_file.sheet_names)} Excel sheets")
            
        elif file_ext == '.csv':
            # Try common encodings for CSV files
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            loaded = False
            
            for encoding in encodings:
                try:
                    self.data = pd.read_csv(self.file_path, encoding=encoding)
                    self.data['_source_sheet'] = 'CSV'
                    print(f"Loaded {len(self.data)} records from CSV using {encoding} encoding")
                    loaded = True
                    break
                except (UnicodeDecodeError, Exception) as e:
                    continue
            
            if not loaded:
                # Last resort: try with error handling
                self.data = pd.read_csv(self.file_path, encoding='utf-8', errors='ignore')
                self.data['_source_sheet'] = 'CSV'
                print(f"Loaded {len(self.data)} records from CSV with error handling")
        else:
            raise ValueError("Unsupported file format. Please provide .xlsx, .xls, or .csv file")
    
    def normalize_columns(self):
        """Normalize column names based on mapping"""
        normalized_cols = {}
        
        for standard_name, possible_names in self.column_mapping.items():
            for col in self.data.columns:
                if col in possible_names:
                    normalized_cols[col] = standard_name
                    break
        
        self.data.rename(columns=normalized_cols, inplace=True)
        print(f"Normalized columns: {list(normalized_cols.values())}")
        
        # Ensure required columns exist
        required = ['name', 'type', 'plan']
        missing = [col for col in required if col not in self.data.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
    
    def extract_device_info(self):
        """Extract device type and actual name from the name field"""
        self.data['name_type_prefix'] = None
        self.data['actual_device_name'] = self.data['name']
        
        # Extract type prefix from name if colon exists
        mask = self.data['name'].astype(str).str.contains(':', na=False)
        self.data.loc[mask, 'name_type_prefix'] = self.data.loc[mask, 'name'].str.split(':', n=1).str[0].str.strip()
        self.data.loc[mask, 'actual_device_name'] = self.data.loc[mask, 'name'].str.split(':', n=1).str[1].str.strip()
        
    def analysis_1_name_type_duplicates(self):
        """Analysis 1: Identify duplicate devices based on name/type combination"""
        print("\n=== Analysis 1: Name/Type Duplicates ===")
        
        results = []
        
        for plan in self.data['plan'].unique():
            plan_data = self.data[self.data['plan'] == plan].copy()
            
            # Group by actual device name and type
            grouped = plan_data.groupby(['actual_device_name', 'type'])
            
            for (device_name, device_type), group in grouped:
                if len(group) > 1:
                    # Found duplicates - now determine if they're exact or mismatch duplicates
                    
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
                    if 'manual_entry' in self.data.columns:
                        manual_entries = group[group['manual_entry'].astype(str).str.upper().isin(['TRUE', 'T', '1', 'YES'])]['name'].tolist()
                    
                    for idx, row in group.iterrows():
                        # Check for type mismatch between prefix and actual type
                        type_mismatch = 'No'
                        if pd.notna(row.get('name_type_prefix')):
                            prefix_upper = str(row.get('name_type_prefix')).upper()
                            type_upper = str(device_type).upper()
                            # Check if prefix is NOT contained in the type
                            if prefix_upper not in type_upper and not any(word in type_upper for word in prefix_upper.split()):
                                type_mismatch = 'Yes'
                        
                        results.append({
                            'Plan': plan,
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
                            'Type Mismatch (Prefix vs Type)': type_mismatch
                        })
        
        df_results = pd.DataFrame(results)
        
        # Print summary stats
        if not df_results.empty:
            print(f"\nTotal duplicate records: {len(df_results)}")
            print(f"\nBreakdown by Duplicate Type:")
            type_counts = df_results.groupby('Duplicate Type').size()
            for dup_type, count in type_counts.items():
                print(f"  - {dup_type}: {count} records")
            
            # Show unique device count for each type
            print(f"\nUnique Devices by Duplicate Type:")
            unique_devices = df_results.groupby('Duplicate Type')['Device Name'].nunique()
            for dup_type, count in unique_devices.items():
                print(f"  - {dup_type}: {count} unique devices")
        
        return df_results
    
    def analysis_2_serial_type_duplicates(self):
        """Analysis 2: Identify duplicate devices based on serial number/type"""
        print("\n=== Analysis 2: Serial Number/Type Duplicates ===")
        
        if 'serial_number' not in self.data.columns:
            print("Warning: Serial number column not found. Skipping analysis 2.")
            return pd.DataFrame()
        
        results = []
        
        # Filter out records without serial numbers
        data_with_serial = self.data[self.data['serial_number'].notna() & (self.data['serial_number'] != '')].copy()
        
        for plan in data_with_serial['plan'].unique():
            plan_data = data_with_serial[data_with_serial['plan'] == plan].copy()
            
            # Group by serial number and type
            grouped = plan_data.groupby(['serial_number', 'type'])
            
            for (serial, device_type), group in grouped:
                if len(group) > 1:
                    # Found duplicates - determine type
                    unique_full_names = group['name'].nunique()
                    unique_device_names = group['actual_device_name'].nunique()
                    
                    # Determine duplicate type
                    if unique_full_names == 1:
                        duplicate_type = "Exact Duplicate"
                        duplicate_description = "Same serial, same type, same name (true duplicate)"
                    elif unique_device_names == 1:
                        duplicate_type = "Mismatch Duplicate"
                        duplicate_description = "Same serial/type, different name prefixes"
                    else:
                        duplicate_type = "Other Duplicate"
                        duplicate_description = "Same serial/type, different device names"
                    
                    # Check for manual entries
                    manual_entries = []
                    if 'manual_entry' in self.data.columns:
                        manual_entries = group[group['manual_entry'].astype(str).str.upper().isin(['TRUE', 'T', '1', 'YES'])]['name'].tolist()
                    
                    for idx, row in group.iterrows():
                        results.append({
                            'Plan': plan,
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
        
        # Print summary stats
        if not df_results.empty:
            print(f"\nTotal duplicate records: {len(df_results)}")
            print(f"\nBreakdown by Duplicate Type:")
            type_counts = df_results.groupby('Duplicate Type').size()
            for dup_type, count in type_counts.items():
                print(f"  - {dup_type}: {count} records")
            
            # Show unique serial count for each type
            print(f"\nUnique Serials by Duplicate Type:")
            unique_serials = df_results.groupby('Duplicate Type')['Serial Number'].nunique()
            for dup_type, count in unique_serials.items():
                print(f"  - {dup_type}: {count} unique serial numbers")
        
        return df_results
    
    def analysis_3_future_serial_duplicates(self):
        """Analysis 3: Future state - duplicates based on serial number only (after name fix)"""
        print("\n=== Analysis 3: Future State Serial Number Duplicates ===")
        
        if 'serial_number' not in self.data.columns:
            print("Warning: Serial number column not found. Skipping analysis 3.")
            return pd.DataFrame()
        
        results = []
        
        # Filter out records without serial numbers
        data_with_serial = self.data[self.data['serial_number'].notna() & (self.data['serial_number'] != '')].copy()
        
        for plan in data_with_serial['plan'].unique():
            plan_data = data_with_serial[data_with_serial['plan'] == plan].copy()
            
            # Group by serial number only (future state where name won't have type prefix)
            grouped = plan_data.groupby('serial_number')
            
            for serial, group in grouped:
                if len(group) > 1:
                    # Determine duplicate type
                    unique_types = group['type'].nunique()
                    unique_actual_names = group['actual_device_name'].nunique()
                    
                    if unique_types == 1 and unique_actual_names == 1:
                        duplicate_type = "Exact Duplicate"
                        duplicate_description = "Same serial, same type, same actual name (will be exact duplicate after fix)"
                    elif unique_types > 1:
                        duplicate_type = "Type Conflict"
                        duplicate_description = "Same serial but different CI types (CRITICAL - needs resolution)"
                    else:
                        duplicate_type = "Name Variation"
                        duplicate_description = "Same serial/type but different actual names"
                    
                    # Found duplicates that will still exist after name fix
                    manual_entries = []
                    if 'manual_entry' in self.data.columns:
                        manual_entries = group[group['manual_entry'].astype(str).str.upper().isin(['TRUE', 'T', '1', 'YES'])]['name'].tolist()
                    
                    for idx, row in group.iterrows():
                        results.append({
                            'Plan': plan,
                            'Serial Number': serial,
                            'Device Name (Current)': row['name'],
                            'Device Name (Future)': row.get('actual_device_name', ''),
                            'Type': row.get('type', ''),
                            'Manual Entry': row.get('manual_entry', ''),
                            'Environment': row.get('environment', ''),
                            'Duplicate Count': len(group),
                            'Duplicate Type': duplicate_type,
                            'Duplicate Description': duplicate_description,
                            'Is Manual Entry': 'Yes' if row['name'] in manual_entries else 'No',
                            'Warning': 'Will still be duplicate after name fix'
                        })
        
        df_results = pd.DataFrame(results)
        
        # Print summary stats
        if not df_results.empty:
            print(f"\nTotal duplicate records (post-fix): {len(df_results)}")
            print(f"\nBreakdown by Duplicate Type:")
            type_counts = df_results.groupby('Duplicate Type').size()
            for dup_type, count in type_counts.items():
                print(f"  - {dup_type}: {count} records")
            
            # Show unique serial count for each type
            print(f"\nUnique Serials by Duplicate Type:")
            unique_serials = df_results.groupby('Duplicate Type')['Serial Number'].nunique()
            for dup_type, count in unique_serials.items():
                print(f"  - {dup_type}: {count} unique serial numbers")
        
        return df_results
    
    def analysis_4_manual_non_production(self):
        """Analysis 4: Manual entries that are not in production environment"""
        print("\n=== Analysis 4: Manual Entry Non-Production Devices ===")
        
        if 'manual_entry' not in self.data.columns:
            print("Warning: Manual entry column not found. Skipping analysis 4.")
            return pd.DataFrame()
        
        results = []
        
        # Filter for manual entries
        manual_data = self.data[
            self.data['manual_entry'].astype(str).str.upper().isin(['TRUE', 'T', '1', 'YES'])
        ].copy()
        
        # Filter for non-production environment
        if 'environment' in self.data.columns:
            non_prod_manual = manual_data[
                ~manual_data['environment'].astype(str).str.upper().str.contains('PROD', na=False)
            ].copy()
        else:
            non_prod_manual = manual_data.copy()
            print("Warning: Environment column not found. Showing all manual entries.")
        
        for idx, row in non_prod_manual.iterrows():
            results.append({
                'Plan': row.get('plan', ''),
                'Device Name': row['name'],
                'Actual Device Name': row.get('actual_device_name', ''),
                'Type': row.get('type', ''),
                'Serial Number': row.get('serial_number', ''),
                'Manual Entry': row.get('manual_entry', ''),
                'Environment': row.get('environment', ''),
                'Issue': 'Manual entry in non-production environment on master plan'
            })
        
        return pd.DataFrame(results)
    
    def analysis_5_post_fix_duplicates_summary(self):
        """Analysis 5: Summary of plans that will still have duplicates after fix"""
        print("\n=== Analysis 5: Post-Fix Duplicate Summary by Plan ===")
        
        if 'serial_number' not in self.data.columns:
            print("Warning: Serial number column not found. Skipping analysis 5.")
            return pd.DataFrame()
        
        results = []
        
        # Filter out records without serial numbers
        data_with_serial = self.data[self.data['serial_number'].notna() & (self.data['serial_number'] != '')].copy()
        
        for plan in data_with_serial['plan'].unique():
            plan_data = data_with_serial[data_with_serial['plan'] == plan].copy()
            
            # Count unique devices by serial number
            total_records = len(plan_data)
            unique_serials = plan_data['serial_number'].nunique()
            duplicate_count = total_records - unique_serials
            
            if duplicate_count > 0:
                # Find which serial numbers are duplicated
                serial_counts = plan_data['serial_number'].value_counts()
                duplicated_serials = serial_counts[serial_counts > 1]
                
                results.append({
                    'Plan': plan,
                    'Total CI Records': total_records,
                    'Unique Serial Numbers': unique_serials,
                    'Duplicate Records (will remain after fix)': duplicate_count,
                    'Number of Duplicated Serial Numbers': len(duplicated_serials),
                    'Duplicate Percentage': f"{(duplicate_count/total_records)*100:.2f}%",
                    'Action Required': 'Yes - Manual cleanup needed'
                })
        
        df_results = pd.DataFrame(results)
        if not df_results.empty:
            df_results = df_results.sort_values('Duplicate Records (will remain after fix)', ascending=False)
        
        return df_results
    
    def analysis_6_same_serial_different_type(self):
        """Analysis 6: Devices with same serial number but different types (Data Quality Issue)"""
        print("\n=== Analysis 6: Same Serial Different Type (Data Quality Issue) ===")
        
        if 'serial_number' not in self.data.columns:
            print("Warning: Serial number column not found. Skipping analysis 6.")
            return pd.DataFrame()
        
        results = []
        
        # Filter out records without serial numbers
        data_with_serial = self.data[self.data['serial_number'].notna() & (self.data['serial_number'] != '')].copy()
        
        for plan in data_with_serial['plan'].unique():
            plan_data = data_with_serial[data_with_serial['plan'] == plan].copy()
            
            # Group by serial number and check if multiple types exist
            for serial, group in plan_data.groupby('serial_number'):
                unique_types = group['type'].nunique()
                
                if unique_types > 1:
                    # Same serial but different types - data quality issue!
                    types_list = group['type'].unique().tolist()
                    
                    for idx, row in group.iterrows():
                        results.append({
                            'Plan': plan,
                            'Serial Number': serial,
                            'Device Name': row['name'],
                            'Actual Device Name': row.get('actual_device_name', ''),
                            'Type': row.get('type', ''),
                            'All Types for Serial': ', '.join(map(str, types_list)),
                            'Type Count': unique_types,
                            'Manual Entry': row.get('manual_entry', ''),
                            'Environment': row.get('environment', ''),
                            'Issue': 'CRITICAL: Same serial number assigned to different CI types',
                            'Explanation': 'This explains discrepancy between Analysis 2 and 3'
                        })
        
        return pd.DataFrame(results)
    
    def run_all_analyses(self):
        """Run all analyses and save to Excel"""
        print(f"\nStarting analysis for file: {self.file_path}")
        print(f"Total records: {len(self.data)}")
        print(f"Unique plans: {self.data['plan'].nunique()}")
        
        # Run all analyses
        df_analysis1 = self.analysis_1_name_type_duplicates()
        df_analysis2 = self.analysis_2_serial_type_duplicates()
        df_analysis3 = self.analysis_3_future_serial_duplicates()
        df_analysis4 = self.analysis_4_manual_non_production()
        df_analysis5 = self.analysis_5_post_fix_duplicates_summary()
        df_analysis6 = self.analysis_6_same_serial_different_type()
        
        # Generate output file path
        input_path = Path(self.file_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = input_path.parent / f"DR_Plan_Analysis_{timestamp}.xlsx"
        
        # Write to Excel with multiple sheets
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Summary sheet
            summary_data = {
                'Analysis': [
                    'Analysis 1: Name/Type Duplicates',
                    'Analysis 2: Serial/Type Duplicates',
                    'Analysis 3: Future Serial Duplicates',
                    'Analysis 4: Manual Non-Prod Entries',
                    'Analysis 5: Post-Fix Summary',
                    'Analysis 6: Same Serial Different Type'
                ],
                'Record Count': [
                    len(df_analysis1),
                    len(df_analysis2),
                    len(df_analysis3),
                    len(df_analysis4),
                    len(df_analysis5),
                    len(df_analysis6)
                ],
                'Sheet Name': [
                    '1_NameType_Duplicates',
                    '2_SerialType_Duplicates',
                    '3_Future_Serial_Dups',
                    '4_Manual_NonProd',
                    '5_PostFix_Summary',
                    '6_Serial_TypeMismatch'
                ],
                'Description': [
                    'Duplicates based on device name and type',
                    'Duplicates based on serial number and type',
                    'Duplicates that will remain after name fix (by serial only)',
                    'Manual entries in non-production environments',
                    'Summary of plans needing post-fix cleanup',
                    'Same serial with different types (explains Analysis 2 vs 3 gap)'
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Write each analysis to a separate sheet
            if not df_analysis1.empty:
                df_analysis1.to_excel(writer, sheet_name='1_NameType_Duplicates', index=False)
            
            if not df_analysis2.empty:
                df_analysis2.to_excel(writer, sheet_name='2_SerialType_Duplicates', index=False)
            
            if not df_analysis3.empty:
                df_analysis3.to_excel(writer, sheet_name='3_Future_Serial_Dups', index=False)
            
            if not df_analysis4.empty:
                df_analysis4.to_excel(writer, sheet_name='4_Manual_NonProd', index=False)
            
            if not df_analysis5.empty:
                df_analysis5.to_excel(writer, sheet_name='5_PostFix_Summary', index=False)
            
            if not df_analysis6.empty:
                df_analysis6.to_excel(writer, sheet_name='6_Serial_TypeMismatch', index=False)
        
        print(f"\n{'='*60}")
        print(f"Analysis complete! Results saved to:")
        print(f"{output_file}")
        print(f"{'='*60}\n")
        
        # Print summary
        print("SUMMARY OF FINDINGS:")
        print(f"  Analysis 1 - Name/Type Duplicates: {len(df_analysis1)} duplicate records found")
        if not df_analysis1.empty:
            for dup_type, count in df_analysis1['Duplicate Type'].value_counts().items():
                print(f"    • {dup_type}: {count} records")
        
        print(f"\n  Analysis 2 - Serial/Type Duplicates: {len(df_analysis2)} duplicate records found")
        if not df_analysis2.empty:
            for dup_type, count in df_analysis2['Duplicate Type'].value_counts().items():
                print(f"    • {dup_type}: {count} records")
        
        print(f"\n  Analysis 3 - Future Serial Duplicates: {len(df_analysis3)} records will still be duplicates")
        if not df_analysis3.empty:
            for dup_type, count in df_analysis3['Duplicate Type'].value_counts().items():
                print(f"    • {dup_type}: {count} records")
        
        print(f"\n  Analysis 4 - Manual Non-Prod: {len(df_analysis4)} manual entries in non-production")
        print(f"  Analysis 5 - Post-Fix Summary: {len(df_analysis5)} plans will need cleanup after fix")
        print(f"  Analysis 6 - Same Serial Different Type: {len(df_analysis6)} data quality issues")
        
        # Highlight the discrepancy explanation
        if len(df_analysis3) != len(df_analysis2):
            diff = abs(len(df_analysis3) - len(df_analysis2))
            print(f"\n⚠️  DISCREPANCY ALERT:")
            print(f"  Analysis 2 vs 3 difference: {diff} records")
            print(f"  Check Analysis 6 - likely caused by same serial having different types")
            print(f"  This is a critical data quality issue that needs resolution!")
        

def main():
    print("="*60)
    print("DR Master Plan CI Duplicate Analysis Tool")
    print("="*60)
    
    # Option 1: Hardcode file path (uncomment and modify the line below)
    # file_path = r"C:\path\to\your\file.xlsx"
    
    # Option 2: Ask for file path
    file_path = input("\nEnter the full path to your Excel or CSV file: ").strip().strip('"')
    
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return
    
    try:
        # Initialize analyzer
        analyzer = DRPlanAnalyzer(file_path)
        
        # Load and process data
        analyzer.load_data()
        analyzer.normalize_columns()
        analyzer.extract_device_info()
        
        # Run all analyses
        analyzer.run_all_analyses()
        
    except Exception as e:
        print(f"\nError occurred during analysis: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
