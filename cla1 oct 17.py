import pandas as pd
import os
from pathlib import Path
import sys
from collections import defaultdict

class DRPlanAnalyzer:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        self.ci_type_column = None
        self.valid_ci_types = ['AIX Server', 'Windows Server', 'Unix Server', 'Linux Server', 
                               'Server', 'Database', 'Application Server', 'Web Server']
    
    def load_data(self):
        """Load Excel or CSV file with proper encoding detection"""
        file_ext = Path(self.file_path).suffix.lower()
        
        try:
            if file_ext in ['.xlsx', '.xls']:
                # Read all sheets from Excel
                excel_file = pd.ExcelFile(self.file_path)
                print(f"Found {len(excel_file.sheet_names)} sheet(s): {excel_file.sheet_names}")
                
                # Combine all sheets
                all_data = []
                for sheet_name in excel_file.sheet_names:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    df['_source_sheet'] = sheet_name
                    all_data.append(df)
                    print(f"  - Sheet '{sheet_name}': {len(df)} records")
                
                self.data = pd.concat(all_data, ignore_index=True)
                
            elif file_ext == '.csv':
                # Try common encodings in order
                encodings_to_try = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252', 'windows-1252']
                
                for enc in encodings_to_try:
                    try:
                        self.data = pd.read_csv(self.file_path, encoding=enc)
                        print(f"✓ Successfully loaded CSV with {enc} encoding")
                        break
                    except (UnicodeDecodeError, UnicodeError):
                        print(f"✗ Failed with {enc} encoding, trying next...")
                        continue
                    except Exception as e:
                        print(f"✗ Error with {enc}: {str(e)}")
                        continue
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            
            if self.data is None:
                raise ValueError("Could not load file with any encoding")
            
            print(f"\nTotal records loaded: {len(self.data)}")
            print(f"Columns found: {list(self.data.columns)}")
            
            # Identify the correct Type column
            self._identify_type_column()
            
            return True
            
        except Exception as e:
            print(f"Error loading file: {str(e)}")
            return False
    
    def _identify_type_column(self):
        """Identify which 'Type' column contains CI types vs scope types"""
        type_columns = [col for col in self.data.columns if 'type' in col.lower()]
        
        print(f"\nFound {len(type_columns)} column(s) with 'Type' in name: {type_columns}")
        
        for col in type_columns:
            unique_values = self.data[col].dropna().unique()[:20]  # Sample first 20
            print(f"\nColumn '{col}' sample values: {unique_values}")
            
            # Check if this column contains CI types (servers, databases, etc.)
            ci_type_matches = sum(1 for val in unique_values 
                                 if any(ci_type.lower() in str(val).lower() 
                                       for ci_type in self.valid_ci_types))
            
            # Check if this column contains scope types
            scope_matches = sum(1 for val in unique_values 
                               if any(scope in str(val).lower() 
                                     for scope in ['primary scope', 'related asset', 'scope']))
            
            if ci_type_matches > scope_matches:
                self.ci_type_column = col
                print(f"\n✓ Using '{col}' as CI Type column (contains CI types like Server, AIX Server, etc.)")
            else:
                print(f"✗ Skipping '{col}' (appears to be scope/relationship type)")
        
        if not self.ci_type_column:
            # Fallback: use first Type column
            self.ci_type_column = type_columns[0] if type_columns else 'Type'
            print(f"\n⚠ Warning: Could not auto-detect CI Type column. Using '{self.ci_type_column}'")
    
    def extract_device_info(self, device_name):
        """Extract type prefix and actual name from device naming convention"""
        if pd.isna(device_name) or not isinstance(device_name, str):
            return None, str(device_name)
        
        if ':' in device_name:
            parts = device_name.split(':', 1)
            return parts[0].strip(), parts[1].strip()
        else:
            return None, device_name.strip()
    
    def analyze_duplicates_current(self):
        """Analyze duplicates based on current naming convention (Type:Name)"""
        print("\n" + "="*80)
        print("ANALYSIS 1: DUPLICATE DEVICES ON MASTER PLANS (CURRENT NAMING)")
        print("="*80)
        
        if 'Name' not in self.data.columns:
            print("Error: 'Name' column not found!")
            return None
        
        if self.ci_type_column not in self.data.columns:
            print(f"Error: CI Type column '{self.ci_type_column}' not found!")
            return None
        
        # Add extracted information
        self.data['name_type_prefix'] = self.data['Name'].apply(lambda x: self.extract_device_info(x)[0])
        self.data['actual_device_name'] = self.data['Name'].apply(lambda x: self.extract_device_info(x)[1])
        
        # Identify master plan column
        plan_columns = [col for col in self.data.columns if 'plan' in col.lower() or 'master' in col.lower()]
        if not plan_columns:
            print("Warning: No 'plan' column found. Using full dataset.")
            plan_column = None
        else:
            plan_column = plan_columns[0]
            print(f"Using '{plan_column}' as Master Plan identifier")
        
        duplicates_report = []
        
        # Group by plan (or analyze all if no plan column)
        if plan_column:
            plans = self.data[plan_column].dropna().unique()
        else:
            plans = ['ALL_DATA']
            self.data['_temp_plan'] = 'ALL_DATA'
            plan_column = '_temp_plan'
        
        for plan in plans:
            plan_data = self.data[self.data[plan_column] == plan].copy()
            
            if len(plan_data) == 0:
                continue
            
            # Find duplicates: same actual_device_name but different name_type_prefix or CI type
            for device_name in plan_data['actual_device_name'].unique():
                device_records = plan_data[plan_data['actual_device_name'] == device_name]
                
                if len(device_records) > 1:
                    # Check if there are mismatches
                    unique_prefixes = device_records['name_type_prefix'].nunique()
                    unique_types = device_records[self.ci_type_column].nunique()
                    
                    if unique_prefixes > 1 or unique_types > 1:
                        for idx, row in device_records.iterrows():
                            duplicates_report.append({
                                'Master Plan': plan,
                                'Full Device Name': row['Name'],
                                'Actual Device Name': device_name,
                                'Name Type Prefix': row['name_type_prefix'],
                                'CI Type Column': row[self.ci_type_column],
                                'Type Mismatch': row['name_type_prefix'] != str(row[self.ci_type_column]).split()[0] if row['name_type_prefix'] else 'No Prefix',
                                'Duplicate Count': len(device_records)
                            })
        
        duplicates_df = pd.DataFrame(duplicates_report)
        
        if len(duplicates_df) > 0:
            print(f"\n🔍 Found {len(duplicates_df)} duplicate device entries across {len(duplicates_df['Master Plan'].unique())} plan(s)")
            
            # Summary by plan
            print("\n📊 Duplicates by Master Plan:")
            plan_summary = duplicates_df.groupby('Master Plan')['Actual Device Name'].nunique().sort_values(ascending=False)
            for plan, count in plan_summary.items():
                print(f"  - {plan}: {count} unique devices with duplicates")
            
            # Show some examples
            print("\n📋 Sample Duplicate Entries (first 10):")
            print(duplicates_df.head(10).to_string(index=False))
            
            return duplicates_df
        else:
            print("\n✅ No duplicates found!")
            return None
    
    def analyze_duplicates_future(self):
        """Analyze what duplicates would exist after fix (Name without type prefix)"""
        print("\n" + "="*80)
        print("ANALYSIS 2: DUPLICATE CHECK FOR FUTURE STATE (NAME ONLY)")
        print("="*80)
        
        # In future state, we only look at actual device name + CI type
        plan_columns = [col for col in self.data.columns if 'plan' in col.lower() or 'master' in col.lower()]
        plan_column = plan_columns[0] if plan_columns else '_temp_plan'
        
        if plan_column == '_temp_plan':
            self.data['_temp_plan'] = 'ALL_DATA'
        
        plans = self.data[plan_column].dropna().unique()
        future_duplicates = []
        
        for plan in plans:
            plan_data = self.data[self.data[plan_column] == plan].copy()
            
            # Group by actual device name and CI type
            grouped = plan_data.groupby(['actual_device_name', self.ci_type_column]).size().reset_index(name='count')
            duplicates = grouped[grouped['count'] > 1]
            
            for idx, dup in duplicates.iterrows():
                matching_records = plan_data[
                    (plan_data['actual_device_name'] == dup['actual_device_name']) &
                    (plan_data[self.ci_type_column] == dup[self.ci_type_column])
                ]
                
                for _, record in matching_records.iterrows():
                    future_duplicates.append({
                        'Master Plan': plan,
                        'Device Name (Future)': dup['actual_device_name'],
                        'CI Type': dup[self.ci_type_column],
                        'Current Full Name': record['Name'],
                        'Duplicate Count': dup['count']
                    })
        
        future_dup_df = pd.DataFrame(future_duplicates)
        
        if len(future_dup_df) > 0:
            print(f"\n⚠️  Found {len(future_dup_df)} entries that would still be duplicates after fix")
            print("   (These have same device name AND same CI type on same plan)")
            
            print("\n📋 Sample Future Duplicates (first 10):")
            print(future_dup_df.head(10).to_string(index=False))
            
            return future_dup_df
        else:
            print("\n✅ After fix, no duplicates expected!")
            return None
    
    def analyze_manual_entries(self):
        """Analyze manually entered devices in non-production environments"""
        print("\n" + "="*80)
        print("ANALYSIS 3: MANUAL ENTRIES IN NON-PRODUCTION ENVIRONMENTS")
        print("="*80)
        
        # Find manual entry column
        manual_columns = [col for col in self.data.columns 
                         if 'manual' in col.lower() or 'entry' in col.lower()]
        
        if not manual_columns:
            print("⚠️  Warning: No 'manual entry' column found. Skipping this analysis.")
            return None
        
        manual_column = manual_columns[0]
        print(f"Using '{manual_column}' as manual entry indicator")
        
        # Find environment column
        env_columns = [col for col in self.data.columns 
                      if 'environment' in col.lower() or 'env' in col.lower()]
        
        if not env_columns:
            print("⚠️  Warning: No environment column found.")
            env_column = None
        else:
            env_column = env_columns[0]
            print(f"Using '{env_column}' as environment indicator")
        
        # Filter for manual entries
        manual_entries = self.data[
            (self.data[manual_column].astype(str).str.lower().isin(['true', '1', 'yes', 't']))
        ].copy()
        
        print(f"\n📊 Total manual entries: {len(manual_entries)}")
        
        if env_column:
            # Find non-production manual entries
            non_prod_manual = manual_entries[
                (~manual_entries[env_column].astype(str).str.lower().str.contains('prod', na=False)) |
                (manual_entries[env_column].isna())
            ].copy()
            
            print(f"🔍 Manual entries in non-production/blank environments: {len(non_prod_manual)}")
            
            if len(non_prod_manual) > 0:
                # Create report
                plan_columns = [col for col in self.data.columns if 'plan' in col.lower()]
                plan_column = plan_columns[0] if plan_columns else None
                
                report_columns = ['Name', self.ci_type_column, env_column, manual_column]
                if plan_column:
                    report_columns.insert(0, plan_column)
                
                available_columns = [col for col in report_columns if col in non_prod_manual.columns]
                
                print("\n📋 Sample Manual Non-Production Entries (first 15):")
                print(non_prod_manual[available_columns].head(15).to_string(index=False))
                
                # Environment breakdown
                print("\n📊 Breakdown by Environment:")
                env_counts = non_prod_manual[env_column].fillna('BLANK').value_counts()
                for env, count in env_counts.items():
                    print(f"  - {env}: {count} devices")
                
                return non_prod_manual[available_columns]
            else:
                print("\n✅ No manual entries found in non-production environments!")
                return None
        else:
            print("\n⚠️  Cannot determine production status without environment column")
            return manual_entries
    
    def generate_report(self, output_dir='./analysis_output'):
        """Generate comprehensive Excel report with all analyses"""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(output_dir, f'DR_Plan_Analysis_{timestamp}.xlsx')
        
        print("\n" + "="*80)
        print("GENERATING COMPREHENSIVE REPORT")
        print("="*80)
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Analysis 1: Current duplicates
            dup_current = self.analyze_duplicates_current()
            if dup_current is not None:
                dup_current.to_excel(writer, sheet_name='Current_Duplicates', index=False)
            
            # Analysis 2: Future duplicates
            dup_future = self.analyze_duplicates_future()
            if dup_future is not None:
                dup_future.to_excel(writer, sheet_name='Future_Duplicates', index=False)
            
            # Analysis 3: Manual entries
            manual_issues = self.analyze_manual_entries()
            if manual_issues is not None:
                manual_issues.to_excel(writer, sheet_name='Manual_NonProd_Issues', index=False)
            
            # Summary sheet
            summary_data = {
                'Analysis': ['Current Duplicates', 'Future Duplicates', 'Manual Non-Prod Entries', 'Total Records Analyzed'],
                'Count': [
                    len(dup_current) if dup_current is not None else 0,
                    len(dup_future) if dup_future is not None else 0,
                    len(manual_issues) if manual_issues is not None else 0,
                    len(self.data)
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        print(f"\n✅ Report generated successfully: {output_file}")
        return output_file


def main():
    print("="*80)
    print("DR MASTER PLAN CI ANALYZER")
    print("="*80)
    
    # Get file path
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = input("\nEnter the path to your Excel/CSV file: ").strip().strip('"').strip("'")
    
    if not os.path.exists(file_path):
        print(f"\n❌ Error: File not found: {file_path}")
        return
    
    # Initialize analyzer
    analyzer = DRPlanAnalyzer(file_path)
    
    # Load data
    if not analyzer.load_data():
        print("\n❌ Failed to load data. Exiting.")
        return
    
    # Generate report
    output_file = analyzer.generate_report()
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)
    print(f"\n📄 Full report saved to: {output_file}")
    print("\nThe report contains the following sheets:")
    print("  1. Summary - Overview of all findings")
    print("  2. Current_Duplicates - Devices with type/name mismatches")
    print("  3. Future_Duplicates - Duplicates that would exist after fix")
    print("  4. Manual_NonProd_Issues - Manual entries in non-production environments")


if __name__ == "__main__":
    main()