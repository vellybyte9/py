import pandas as pd
import sys
from tkinter import Tk, filedialog

def analyze_servicenow_data(file_path):
    # Determine file type
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
        sheets = {'Sheet1': df}
    else:
        sheets = pd.read_excel(file_path, sheet_name=None)
        
    all_results = []

    for sheet_name, df in sheets.items():
        print(f"Processing sheet: {sheet_name}")
        df = df.copy()

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower()

        # Identify the correct 'type' column
        type_cols = [col for col in df.columns if 'type' in col]
        if len(type_cols) > 1:
            type_counts = df[type_cols].apply(lambda x: x.dropna().astype(str).str.contains('Server', case=False, na=False).sum())
            ci_type_col = type_counts.idxmax()
        else:
            ci_type_col = type_cols[0]

        # Check for required columns
        required_cols = ['device name', ci_type_col, 'plan', 'server environment', 'manual entry']
        for col in required_cols:
            if col not in df.columns:
                print(f"Warning: Column '{col}' not found in {sheet_name}")
                continue

        # Split device name into prefix and core name
        df['name_prefix'] = df['device name'].astype(str).str.split(':').str[0].str.strip()
        df['device_core_name'] = df['device name'].astype(str).str.split(':').str[1:].str.join(':').str.strip()

        # Identify duplicates by plan + core name + type
        df['plan'] = df['plan'].astype(str)
        duplicates = df.groupby(['plan', 'device_core_name', ci_type_col]).filter(lambda x: len(x) > 1)

        # Identify mismatched type prefix vs. CI type
        df['type_mismatch'] = df.apply(lambda x: x['name_prefix'].lower() not in str(x[ci_type_col]).lower(), axis=1)
        type_mismatch_records = df[df['type_mismatch']]

        # Find manual entries not in production
        if 'manual entry' in df.columns and 'server environment' in df.columns:
            manual_issue_records = df[
                (df['manual entry'].astype(str).str.lower() == 'true') &
                (~df['server environment'].astype(str).str.lower().isin(['production', 'prod']))
            ]
        else:
            manual_issue_records = pd.DataFrame()

        results = {
            'Sheet': sheet_name,
            'Duplicate Records': duplicates,
            'Type Mismatch Records': type_mismatch_records,
            'Manual Entry Non-Prod Records': manual_issue_records
        }
        all_results.append(results)

    # Write results to Excel file
    with pd.ExcelWriter('ServiceNow_Device_Analysis_Output.xlsx', engine='xlsxwriter') as writer:
        for res in all_results:
            res['Duplicate Records'].to_excel(writer, sheet_name=f"{res['Sheet']}_Duplicates", index=False)
            res['Type Mismatch Records'].to_excel(writer, sheet_name=f"{res['Sheet']}_TypeMismatch", index=False)
            res['Manual Entry Non-Prod Records'].to_excel(writer, sheet_name=f"{res['Sheet']}_ManualNonProd", index=False)

    print('Analysis complete. Results written to ServiceNow_Device_Analysis_Output.xlsx')

if __name__ == '__main__':
    # Allow file path via command line or file dialog
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        print("No input file provided in command line. Please select a file.")
        Tk().withdraw()
        file_path = filedialog.askopenfilename(
            title="Select your ServiceNow export file",
            filetypes=[("Excel or CSV files", "*.xlsx *.xls *.csv")]
        )
        if not file_path:
            print("No file selected. Exiting.")
            sys.exit(1)

    analyze_servicenow_data(file_path)
