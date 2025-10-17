import pandas as pd
import os

# Main function to analyze ServiceNow DR Master Plan extract
def analyze_servicenow_extract(file_path):
    # Handle both Excel and CSV files
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path, dtype=str)
        sheets = {'Sheet1': df}
    else:
        excel = pd.ExcelFile(file_path)
        sheets = {sheet: pd.read_excel(file_path, sheet_name=sheet, dtype=str) for sheet in excel.sheet_names}

    all_results = []

    for sheet_name, df in sheets.items():
        print(f"\nAnalyzing sheet: {sheet_name}")

        # Clean up column names
        df.columns = [c.strip().lower() for c in df.columns]

        # Identify relevant columns
        type_cols = [c for c in df.columns if 'type' in c]
        real_type_col = None
        for col in type_cols:
            if df[col].str.contains('server', case=False, na=False).any():
                real_type_col = col
                break

        if not real_type_col:
            print(f"No valid type column found in sheet {sheet_name}, skipping.")
            continue

        # Detect likely column names
        name_col = next((c for c in df.columns if 'name' in c), None)
        plan_col = next((c for c in df.columns if 'plan' in c), None)
        env_col = next((c for c in df.columns if 'environment' in c), None)
        manual_col = next((c for c in df.columns if 'manual' in c), None)

        if not all([name_col, plan_col]):
            print(f"Missing required columns for sheet {sheet_name}, skipping.")
            continue

        # Normalize strings
        df[[real_type_col, name_col, plan_col]] = df[[real_type_col, name_col, plan_col]].apply(lambda x: x.str.strip().str.lower())
        if env_col:
            df[env_col] = df[env_col].str.strip().str.lower()
        if manual_col:
            df[manual_col] = df[manual_col].str.strip().str.lower()

        # Extract actual device name (after colon)
        df['parsed_name'] = df[name_col].str.split(':').str[-1].str.strip()

        # Duplicate detection logic (per plan)
        df['unique_key'] = df[real_type_col] + ':' + df['parsed_name'] + ':' + df[plan_col]
        duplicates = df[df.duplicated('unique_key', keep=False)].sort_values(by=[plan_col, 'parsed_name'])

        duplicate_summary = duplicates[[plan_col, name_col, real_type_col, 'parsed_name']].copy()
        duplicate_summary['issue'] = 'Duplicate device within same plan'

        # Identify manual non-production records
        manual_issues = pd.DataFrame()
        if manual_col and env_col:
            manual_issues = df[
                (df[manual_col] == 'true') &
                ((df[env_col].isna()) | (~df[env_col].str.contains('prod')))
            ][[plan_col, name_col, real_type_col, env_col, manual_col]].copy()
            manual_issues['issue'] = 'Manual entry in non-production or blank env'

        # Combine results
        sheet_result = pd.concat([duplicate_summary, manual_issues], ignore_index=True)
        sheet_result['sheet_name'] = sheet_name
        all_results.append(sheet_result)

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
    else:
        final_df = pd.DataFrame()

    # Save results
    out_path = os.path.splitext(file_path)[0] + '_analysis_results.xlsx'
    final_df.to_excel(out_path, index=False)
    print(f"Analysis complete. Output saved to: {out_path}")

    return out_path
