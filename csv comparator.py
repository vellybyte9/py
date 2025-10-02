import csv
import sys

def get_csv_columns(filepath):
    """Extract column headers from a CSV file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            # Strip whitespace from headers
            return [h.strip() for h in headers]
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading '{filepath}': {e}")
        sys.exit(1)

def compare_columns(api_file, frontend_file):
    """Compare columns between API data and frontend table."""
    print("=" * 70)
    print("CSV COLUMN COMPARISON: API vs ServiceNow Frontend")
    print("=" * 70)
    
    # Get columns from both files
    api_columns = get_csv_columns(api_file)
    frontend_columns = get_csv_columns(frontend_file)
    
    api_set = set(api_columns)
    frontend_set = set(frontend_columns)
    
    # Print column counts
    print(f"\nAPI CSV columns: {len(api_columns)}")
    print(f"Frontend CSV columns: {len(frontend_columns)}")
    
    # Find differences
    missing_in_api = frontend_set - api_set
    extra_in_api = api_set - frontend_set
    common_columns = api_set & frontend_set
    
    # Results
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    
    print(f"\n✓ Common columns: {len(common_columns)}")
    if common_columns:
        for col in sorted(common_columns):
            print(f"  • {col}")
    
    print(f"\n⚠ Missing in API (present in frontend): {len(missing_in_api)}")
    if missing_in_api:
        for col in sorted(missing_in_api):
            print(f"  • {col}")
    else:
        print("  None - API has all frontend columns!")
    
    print(f"\n⚡ Extra in API (not in frontend): {len(extra_in_api)}")
    if extra_in_api:
        for col in sorted(extra_in_api):
            print(f"  • {col}")
    else:
        print("  None")
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    if not missing_in_api:
        print("✓ SUCCESS: API data contains all frontend columns!")
    else:
        print(f"⚠ WARNING: {len(missing_in_api)} frontend column(s) missing from API data")
    print("=" * 70)

if __name__ == "__main__":
    # Update these paths to your CSV files
    API_CSV = "api_data.csv"
    FRONTEND_CSV = "frontend_columns.csv"
    
    print("\nComparing CSV columns...")
    print(f"API CSV: {API_CSV}")
    print(f"Frontend CSV: {FRONTEND_CSV}")
    
    compare_columns(API_CSV, FRONTEND_CSV)
