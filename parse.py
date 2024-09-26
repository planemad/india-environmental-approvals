import os
import json
import polars as pl
from typing import Dict, Any, Union

# Directory path
directory = "raw/caf"

def recursive_find_json(directory: str) -> list[str]:
    """Recursively finds JSON files in the given directory."""
    return [os.path.join(root, file) for root, _, files in os.walk(directory) for file in files if file.endswith('.json')]

def parse_json(file_path: str) -> Dict[str, Any]:
    """Parses a JSON file and extracts specified keys."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        result = extract_values(data)
        proposal_id = file_path.split('/')[-1].strip('.json')
        result['proposal_url'] = f"https://parivesh.nic.in/newupgrade/#/report/ec?proposalId={proposal_id}"
        
        return result
    
    except json.JSONDecodeError:
        print(f"Error decoding JSON in {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
    
    return {}

def safe_get(d: Union[Dict, list], *keys) -> Any:
    """Safely navigate nested dictionaries and lists."""
    for key in keys:
        if isinstance(d, dict):
            if key in d:
                d = d[key]
            else:
                return None
        elif isinstance(d, list):
            if isinstance(key, int) and 0 <= key < len(d):
                d = d[key]
            else:
                return None
        else:
            return None
    return d

def extract_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts available values from the data"""
    results = {}
    
    fields_to_extract = {
        'Proposal Number': ('data', 'proponentApplications', 'proposal_no'),
        'Application Date': ('data', 'proponentApplications', 'created_on'),
        'Project Name': ('data', 'proponentApplications', 'projectDetailDto', 'projectName'),
        'Project Description': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'project_description'),
        'State': ('data', 'proponentApplications', 'state'),
        'District': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafKML', 0, 'cafKMLPlots', 0, 'district'),
        'Total Cost (Lakhs)': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafProjectActivityCost', 'total_cost'),
        'Employment (Construction)': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafProjectActivityCost', 'cp_total_employment'),
        'Employment (Operational)': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafProjectActivityCost', 'op_existing_total_employment'),
        'Project Land Requirement (Hectares)': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafLocationOfKml', 'existing_total_land'),
        'Organization Name': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'organization_name'),
        'Project Category (Code)': ('data', 'clearence', 'project_category'),
        'Project Category': ('data', 'clearence', 'environmentClearanceProjectActivityDetails', 0, 'activities', 'name'),
    }

    for field, keys in fields_to_extract.items():
        value = safe_get(data, *keys)
        if value is not None:
            results[field] = value

    return results

def main():
    json_files = recursive_find_json(directory)
    
    # Use Polars to efficiently process the data
    df = pl.DataFrame([parse_json(file) for file in json_files])
    
    # Filter rows to keep only those with a valid 'Proposal Number'
    df = df.filter(pl.col('Proposal Number').is_not_null())
    
    # Replace newlines with semicolons in string columns
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.replace_all("\n", ";"))
    
    # Ensure the output directory exists
    os.makedirs("csv", exist_ok=True)
    output_file = "csv/Projects.csv"
    
    # Write to CSV
    df.write_csv(output_file)
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    main()
