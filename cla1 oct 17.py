import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings(‘ignore’)

class DRPlanAnalyzer:
def **init**(self, file_path):
“””
Initialize the analyzer with the file path

```
    Args:
        file_path: Path to Excel or CSV file
    """
    self.file_path = file_path
    self.data = None
    self.results = {}
    
def load_data(self):
    """Load data from Excel or CSV file"""
    print(f"Loading data from: {self.file_path}")
    
    file_ext = Path(self.file_path).suffix.lower()
    
    try:
        if file_ext in ['.xlsx', '.xls']:
            # Try to read Excel file - combine all sheets
            excel_file = pd.ExcelFile(self.file_path)
            print(f"Found {len(excel_file.sheet_names)} sheet(s): {excel_file.sheet_names}")
            
            dfs = []
            for sheet in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet)
                df['source_sheet'] = sheet
                dfs.append(df)
            
            self.data = pd.concat(dfs, ignore_index=True)
            
        elif file_ext == '.csv':
            self.data = pd.read_csv(self.file_path)
            self.data['source_sheet'] = 'CSV'
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
        
        print(f"Successfully loaded {len(self.data):,} records")
        print(f"\nColumns found: {list(self.data.columns)}")
        
        # Standardize column names (case-insensitive matching)
        self.standardize_columns()
        
    except Exception as e:
        print(f"Error loading file: {e}")
        raise

def standardize_columns(self):
    """Standardize column names for easier processing"""
    # Create a mapping of lowercase column names to original names
    col_mapping = {}
    
    for col in self.data.columns:
        col_lower = col.lower().strip()
        
        # Map common variations to standard names
        if 'device' in col_lower and 'name' in col_lower:
            col_mapping[col] = 'device_name'
        elif col_lower in ['type', 'ci type', 'ci_type', 'configuration item type']:
            if 'primary' not in col_lower and 'scope' not in col_lower:
                col_mapping[col] = 'ci_type'
        elif 'plan' in col_lower and ('name' in col_lower or 'id' in col_lower):
            col_mapping[col] = 'plan_name'
        elif 'environment' in col_lower or 'env' in col_lower:
            col_mapping[col] = 'environment'
        elif 'manual' in col_lower and 'entry' in col_lower:
            col_mapping[col] = 'manual_entry'
        elif col_lower in ['scope type', 'scope_type', 'relationship type']:
            col_mapping[col] = 'scope_type'
    
    # Rename columns
    if col_mapping:
        self.data.rename(columns=col_mapping, inplace=True)
        print(f"\nStandardized columns: {col_mapping}")

def extract_device_info(self, device_name):
    """
    Extract type prefix and actual name from device name
    
    Args:
        device_name: Full device name string
        
    Returns:
        tuple: (type_prefix, actual_name)
    """
    if pd.isna(device_name):
        return (None, None)
    
    device_name = str(device_name).strip()
    
    if ':' in device_name:
        parts = device_name.split(':', 1)
        return (parts[0].strip(), parts[1].strip())
    else:
        # No colon found - future format or malformed
        return (None, device_name)

def analyze_duplicates_current_format(self):
    """
    Analyze duplicates in current format where name contains type prefix
    Identifies mismatches between type column and name prefix
    """
    print("\n" + "="*80)
    print("ANALYSIS 1: Duplicate Devices (Current Format)")
    print("="*80)
    
    if 'device_name' not in self.data.columns or 'ci_type' not in self.data.columns:
        print("ERROR: Required columns 'device_name' and/or 'ci_type' not found")
        return
    
    # Extract type prefix and actual name
    self.data['name_prefix'] = self.data['device_name'].apply(
        lambda x: self.extract_device_info(x)[0]
    )
    self.data['actual_name'] = self.data['device_name'].apply(
        lambda x: self.extract_device_info(x)[1]
    )
    
    # Identify type mismatches
    self.data['type_mismatch'] = False
    
    for idx, row in self.data.iterrows():
        ci_type = str(row.get('ci_type', '')).lower().strip()
        name_prefix = str(row.get('name_prefix', '')).lower().strip()
        
        # Check if there's a mismatch
        if name_prefix and ci_type:
            # Handle variations (e.g., "AIX Server" vs "Unix Server")
            if ci_type not in name_prefix and name_prefix not in ci_type:
                self.data.at[idx, 'type_mismatch'] = True
    
    # Find duplicates by actual name within each plan
    duplicates_list = []
    
    if 'plan_name' in self.data.columns:
        for plan in self.data['plan_name'].unique():
            if pd.isna(plan):
                continue
            
            plan_data = self.data[self.data['plan_name'] == plan].copy()
            
            # Group by actual device name
            for actual_name in plan_data['actual_name'].unique():
                if pd.isna(actual_name):
                    continue
                
                device_records = plan_data[plan_data['actual_name'] == actual_name]
                
                # Check if there are multiple records with different types or name prefixes
                unique_types = device_records['ci_type'].nunique()
                unique_prefixes = device_records['name_prefix'].nunique()
                
                if len(device_records) > 1 or unique_types > 1 or unique_prefixes > 1:
                    for _, record in device_records.iterrows():
                        duplicates_list.append({
                            'plan_name': plan,
                            'device_name': record['device_name'],
                            'actual_name': record['actual_name'],
                            'name_prefix': record['name_prefix'],
                            'ci_type': record['ci_type'],
                            'type_mismatch': record['type_mismatch'],
                            'environment': record.get('environment', 'N/A'),
                            'manual_entry': record.get('manual_entry', 'N/A'),
                            'duplicate_count': len(device_records)
                        })
    
    self.results['current_duplicates'] = pd.DataFrame(duplicates_list)
    
    if len(duplicates_list) > 0:
        print(f"\nFound {len(duplicates_list)} duplicate device records across all plans")
        print(f"Unique devices with duplicates: {self.results['current_duplicates']['actual_name'].nunique()}")
        
        # Summary by plan
        print("\n--- Duplicates by Plan ---")
        dup_by_plan = self.results['current_duplicates'].groupby('plan_name').agg({
            'device_name': 'count',
            'actual_name': 'nunique'
        }).rename(columns={
            'device_name': 'total_duplicate_records',
            'actual_name': 'unique_devices_affected'
        })
        print(dup_by_plan.to_string())
        
        # Type mismatches
        mismatches = self.results['current_duplicates'][
            self.results['current_duplicates']['type_mismatch'] == True
        ]
        if len(mismatches) > 0:
            print(f"\n--- Type Mismatches Found: {len(mismatches)} records ---")
            print("\nSample mismatches:")
            print(mismatches[['plan_name', 'device_name', 'name_prefix', 'ci_type']].head(10).to_string())
    else:
        print("\nNo duplicates found in current format")

def analyze_duplicates_future_format(self):
    """
    Analyze duplicates in future format where name has no type prefix
    This will be used after the system fix is implemented
    """
    print("\n" + "="*80)
    print("ANALYSIS 2: Duplicate Check (Future Format - Name Only)")
    print("="*80)
    
    if 'device_name' not in self.data.columns:
        print("ERROR: Required column 'device_name' not found")
        return
    
    # In future format, device_name should not have colon
    # For now, we'll use the extracted actual_name as proxy
    
    duplicates_list = []
    
    if 'plan_name' in self.data.columns:
        for plan in self.data['plan_name'].unique():
            if pd.isna(plan):
                continue
            
            plan_data = self.data[self.data['plan_name'] == plan].copy()
            
            # In future format, check for exact device_name + ci_type duplicates
            # Group by actual_name (or device_name in future) and ci_type
            grouped = plan_data.groupby(['actual_name', 'ci_type']).size().reset_index(name='count')
            
            # Any combination appearing more than once is a duplicate
            dups = grouped[grouped['count'] > 1]
            
            for _, dup_group in dups.iterrows():
                matching_records = plan_data[
                    (plan_data['actual_name'] == dup_group['actual_name']) &
                    (plan_data['ci_type'] == dup_group['ci_type'])
                ]
                
                for _, record in matching_records.iterrows():
                    duplicates_list.append({
                        'plan_name': plan,
                        'device_name': record['device_name'],
                        'actual_name': record['actual_name'],
                        'ci_type': record['ci_type'],
                        'environment': record.get('environment', 'N/A'),
                        'manual_entry': record.get('manual_entry', 'N/A'),
                        'duplicate_count': dup_group['count']
                    })
    
    self.results['future_duplicates'] = pd.DataFrame(duplicates_list)
    
    if len(duplicates_list) > 0:
        print(f"\nFound {len(duplicates_list)} potential duplicate records in future format")
        print(f"Unique device/type combinations with duplicates: {len(dups)}")
        
        print("\n--- Future Format Duplicates by Plan ---")
        dup_by_plan = self.results['future_duplicates'].groupby('plan_name').agg({
            'device_name': 'count'
        }).rename(columns={'device_name': 'duplicate_records'})
        print(dup_by_plan.to_string())
    else:
        print("\nNo duplicates would exist in future format (Good news!)")

def analyze_manual_nonprod_entries(self):
    """
    Analyze manually entered devices in non-production environments
    These should typically not be on master plans
    """
    print("\n" + "="*80)
    print("ANALYSIS 3: Manual Entries in Non-Production Environments")
    print("="*80)
    
    if 'manual_entry' not in self.data.columns:
        print("ERROR: Required column 'manual_entry' not found")
        return
    
    # Convert manual_entry to boolean
    self.data['manual_entry_bool'] = self.data['manual_entry'].astype(str).str.lower().isin(
        ['true', '1', 'yes', 't']
    )
    
    # Filter for manual entries
    manual_entries = self.data[self.data['manual_entry_bool'] == True].copy()
    
    print(f"\nTotal manual entries found: {len(manual_entries):,}")
    
    # Check environment column
    if 'environment' in self.data.columns:
        # Flag non-production or blank environments
        manual_entries['is_nonprod'] = manual_entries['environment'].apply(
            lambda x: pd.isna(x) or 
            str(x).lower().strip() not in ['production', 'prod', 'prd']
        )
        
        nonprod_manual = manual_entries[manual_entries['is_nonprod'] == True]
        
        self.results['manual_nonprod'] = nonprod_manual
        
        print(f"Manual entries in NON-PRODUCTION or blank environment: {len(nonprod_manual):,}")
        
        if len(nonprod_manual) > 0:
            print("\n--- Manual Non-Prod Entries by Plan ---")
            by_plan = nonprod_manual.groupby('plan_name').size().reset_index(name='count')
            print(by_plan.to_string(index=False))
            
            print("\n--- Manual Non-Prod Entries by Environment ---")
            by_env = nonprod_manual.groupby('environment').size().reset_index(name='count')
            print(by_env.to_string(index=False))
            
            print("\n--- Sample Records ---")
            sample_cols = ['plan_name', 'device_name', 'ci_type', 'environment', 'manual_entry']
            available_cols = [col for col in sample_cols if col in nonprod_manual.columns]
            print(nonprod_manual[available_cols].head(10).to_string())
    else:
        print("WARNING: 'environment' column not found. Cannot determine production status")
        self.results['manual_nonprod'] = manual_entries

def generate_report(self, output_dir='analysis_results'):
    """
    Generate Excel report with all analysis results
    
    Args:
        output_dir: Directory to save the report
    """
    print("\n" + "="*80)
    print("Generating Excel Report")
    print("="*80)
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_dir}/DR_Plan_Analysis_{timestamp}.xlsx"
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = []
        summary_data.append(['Analysis Date', datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
        summary_data.append(['Source File', self.file_path])
        summary_data.append(['Total Records', len(self.data)])
        summary_data.append([''])
        
        if 'current_duplicates' in self.results and len(self.results['current_duplicates']) > 0:
            summary_data.append(['Current Format Duplicates', len(self.results['current_duplicates'])])
            summary_data.append(['Unique Devices Affected', 
                               self.results['current_duplicates']['actual_name'].nunique()])
        else:
            summary_data.append(['Current Format Duplicates', 0])
        
        if 'future_duplicates' in self.results and len(self.results['future_duplicates']) > 0:
            summary_data.append(['Future Format Duplicates', len(self.results['future_duplicates'])])
        else:
            summary_data.append(['Future Format Duplicates', 0])
        
        if 'manual_nonprod' in self.results:
            summary_data.append(['Manual Non-Prod Entries', len(self.results['manual_nonprod'])])
        
        summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Write each analysis result to a separate sheet
        if 'current_duplicates' in self.results and len(self.results['current_duplicates']) > 0:
            self.results['current_duplicates'].to_excel(
                writer, sheet_name='Current_Duplicates', index=False
            )
        
        if 'future_duplicates' in self.results and len(self.results['future_duplicates']) > 0:
            self.results['future_duplicates'].to_excel(
                writer, sheet_name='Future_Duplicates', index=False
            )
        
        if 'manual_nonprod' in self.results and len(self.results['manual_nonprod']) > 0:
            self.results['manual_nonprod'].to_excel(
                writer, sheet_name='Manual_NonProd', index=False
            )
    
    print(f"\nReport saved to: {output_file}")
    return output_file

def run_analysis(self):
    """Run all analyses"""
    print("\n" + "="*80)
    print("DR MASTER PLAN ANALYSIS")
    print("="*80)
    
    self.load_data()
    self.analyze_duplicates_current_format()
    self.analyze_duplicates_future_format()
    self.analyze_manual_nonprod_entries()
    
    # Generate report
    report_path = self.generate_report()
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    print(f"\nDetailed results have been saved to: {report_path}")
```

def main():
“”“Main execution function”””
print(“ServiceNow DR Master Plan Analyzer”)
print(”=”*80)

```
# Get file path from user
file_path = input("\nEnter the path to your Excel or CSV file: ").strip().strip('"').strip("'")

if not os.path.exists(file_path):
    print(f"ERROR: File not found: {file_path}")
    return

try:
    analyzer = DRPlanAnalyzer(file_path)
    analyzer.run_analysis()
    
    print("\n\nPress Enter to exit...")
    input()
    
except Exception as e:
    print(f"\nERROR: An error occurred during analysis: {e}")
    import traceback
    traceback.print_exc()
```

if **name** == “**main**”:
main()