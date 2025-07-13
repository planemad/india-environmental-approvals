import os
import json
import polars as pl
import sys
import xml.etree.ElementTree as ET
from typing import Dict, Any, Union

def get_directory_path(state: str = None) -> str:
    """Get the appropriate directory path based on state parameter."""
    if state:
        return f"raw/caf_{state.lower()}"
    return "raw/caf"

# Get state parameter from command line arguments
state_param = sys.argv[1] if len(sys.argv) > 1 else None
directory = get_directory_path(state_param)

print(f"Processing data from directory: {directory}")

if not os.path.exists(directory):
    print(f"Error: Directory {directory} does not exist. Please run initialize.sh and fetch.sh first.")
    sys.exit(1)

def recursive_find_json(directory: str) -> list[str]:
    """Recursively finds JSON files in the given directory."""
    return [os.path.join(root, file) for root, _, files in os.walk(directory) for file in files if file.endswith('.json')]

def parse_xml_content(xml_string: str) -> Dict[str, Any]:
    """Parse XML content and extract fields."""
    try:
        root = ET.fromstring(xml_string)
        result = {}
        
        # Extract direct XML elements
        xml_fields = {
            'nameOfUserAgency': 'Organization Name',
            'state': 'State', 
            'proposalNo': 'Proposal Number',
            'projectName': 'Project Name',
            'category': 'Project Category (Code)',
            'proposalStatus': 'Proposal Status',
            'app_updated_on': 'Application Updated On'
        }
        
        for xml_tag, field_name in xml_fields.items():
            element = root.find(xml_tag)
            if element is not None and element.text:
                result[field_name] = element.text.strip()
        
        # Parse other_property JSON if present
        other_property = root.find('other_property')
        if other_property is not None and other_property.text:
            try:
                properties = json.loads(other_property.text)
                for prop in properties:
                    if prop.get('label') == 'Activity':
                        result['Project Category'] = prop.get('value', '')
                    elif prop.get('label') == 'Sector':
                        result['Sector'] = prop.get('value', '')
            except json.JSONDecodeError:
                pass
        
        return result
    except ET.ParseError:
        return {}

def parse_json(file_path: str) -> Dict[str, Any]:
    """Parses a JSON file and extracts specified keys."""
    try:
        with open(file_path, 'r') as f:
            content = f.read().strip()
        
        # Try to parse as JSON first
        try:
            data = json.loads(content)
            result = extract_values(data)
        except json.JSONDecodeError:
            # If JSON parsing fails, try XML parsing
            result = parse_xml_content(content)
        
        proposal_id = file_path.split('/')[-1].strip('.json')
        result['proposal_url'] = f"https://parivesh.nic.in/newupgrade/#/report/ec?proposalId={proposal_id}"
        
        return result
    
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

def extract_kml_urls(data: Dict[str, Any]) -> list[str]:
    """Extract KML URLs from the data"""
    kml_urls = []
    
    # Navigate to cafKML array
    caf_kml_list = safe_get(data, 'data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafKML')
    if caf_kml_list and isinstance(caf_kml_list, list):
        for caf_kml_item in caf_kml_list:
            if isinstance(caf_kml_item, dict) and 'caf_kml' in caf_kml_item:
                caf_kml = caf_kml_item['caf_kml']
                if isinstance(caf_kml, dict) and 'document_name' in caf_kml:
                    document_name = caf_kml['document_name']
                    if document_name and document_name.endswith('.kml'):
                        # Extract required fields for the URL
                        doc_mapping_id = caf_kml.get('document_mapping_id')
                        ref_id = caf_kml.get('ref_id')
                        ref_type = caf_kml.get('type')
                        uuid = caf_kml.get('uuid')
                        version = caf_kml.get('version')
                        
                        # Construct the KML URL with the correct format
                        if all([doc_mapping_id, ref_id, ref_type, uuid, version]):
                            kml_url = f"https://parivesh.nic.in/dms/okm/downloadDocument?docTypemappingId={doc_mapping_id}&refId={ref_id}&refType={ref_type}&uuid={uuid}&version={version}"
                            kml_urls.append(kml_url)
    
    return kml_urls

def extract_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts available values from the data"""
    results = {}
    
    fields_to_extract = {
        'ID': ('data', 'proponentApplications', 'id'),
        'Category': ('data', 'proponentApplications', 'applications', 'category'),
        'Description': ('data', 'proponentApplications', 'applications', 'description'),


        'Proposal Number': ('data', 'proponentApplications', 'proposal_no'),
        'Application Date': ('data', 'proponentApplications', 'created_on'),
        'Project Name': ('data', 'proponentApplications', 'projectDetailDto', 'projectName'),
        'Project Description': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'project_description'),
        'Total Cost (Lakhs)': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafProjectActivityCost', 'total_cost'),
        'Employment (Construction)': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafProjectActivityCost', 'cp_total_employment'),
        'Employment (Operational)': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafProjectActivityCost', 'op_existing_total_employment'),
        'Project Land Requirement (Hectares)': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafLocationOfKml', 'existing_total_land'),
        'Organization Name': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'organization_name'),
        'Project Category (Code)': ('data', 'clearence', 'project_category'),
        'Project Category': ('data', 'clearence', 'environmentClearanceProjectActivityDetails', 0, 'activities', 'name'),
        # Geographic information fields
        
        'Plot Number': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafKML', 0, 'cafKMLPlots', 0, 'plot_no'),
        'Village': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafKML', 0, 'cafKMLPlots', 0, 'village'),
        'Sub District': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafKML', 0, 'cafKMLPlots', 0, 'sub_District'),
        'District': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafKML', 0, 'cafKMLPlots', 0, 'district'),
        'State': ('data', 'proponentApplications', 'state'),
        'Village Code': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafKML', 0, 'cafKMLPlots', 0, 'village_code'),
        
    
       'Proposal Type': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'proposal_for'),
        'MoEFCC File': ('data', 'proponentApplications', 'moefccFileNumber'),
        'State File': ('data', 'proponentApplications', 'stateFileNumber'),
        'Plot Nos': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafKML', 0, 'cafKMLPlots', 0, 'plot_no'),
        'Shape of Project': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafLocationOfKml', 'shape_of_project'),
        'Existing Non-Forest Land': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafLocationOfKml', 'existing_non_forest_land'),
        'Existing Forest Land': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafLocationOfKml', 'existing_forest_land'),
        'Existing Total Land': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafLocationOfKml', 'existing_total_land'),
        'Additional Non-Forest Land': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafLocationOfKml', 'additional_non_forest_land'),
        'Additional Forest Land': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafLocationOfKml', 'additional_forest_land'),
        'Additional Total Land': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafLocationOfKml', 'additional_total_land'),
        'Existing Cost': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafProjectActivityCost', 'total_existing_cost'),
        'Expansion Cost': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafProjectActivityCost', 'total_expension_cost'),
        'Villages Affected': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafOthers', 'no_of_villages'),
        'Project Displaced Families': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafOthers', 'no_of_project_displaced_families'),
        'Project Affected Families': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafOthers', 'no_of_project_affected_families'),
        'Alternative Site Examined': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafOthers', 'is_alternative_sites_examined'),
        'Alternative Site Description': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafOthers', 'alternative_sites_description'),
        'Government Restriction': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafOthers', 'is_any_govt_restriction'),
        'Litigation Pending': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafOthers', 'is_any_litigation_pending'),
        'Violation Involved': ('data', 'proponentApplications', 'projectDetailDto', 'commonFormDetails', 0, 'cafOthers', 'is_any_violayion_involved'),
        'Last Visible Status': ('data', 'proponentApplications', 'last_visible_status'),
        'Last Submission Date': ('data', 'proponentApplications', 'last_submission_date'),
        'Grant Date': ('data', 'proponentApplications', 'grant_date'),
        'Project Exemption Reason': ('data', 'clearence', 'project_exempted_reason'),
        'EC Consultant': ('data', 'clearence', 'ecConsultant', 'consultant_name'),
        
    }

    for field, keys in fields_to_extract.items():
        value = safe_get(data, *keys)
        if value is not None:
            results[field] = value

    # Extract KML URLs
    kml_urls = extract_kml_urls(data)
    if kml_urls:
        results['KML URLs'] = ';'.join(kml_urls)  # Join multiple URLs with semicolon

    # EIA Report PDF URL
    eia = safe_get(data, 'data', 'proponentApplications', 'ecEnclosures', 'eia_final_copy')
    if eia and isinstance(eia, dict):
        doc_id = eia.get('document_mapping_id')
        ref_id = eia.get('ref_id')
        ref_type = eia.get('type')
        uuid = eia.get('uuid')
        version = eia.get('version')
        if all([doc_id, ref_id, ref_type, uuid, version]):
            eia_url = f"https://parivesh.nic.in/dms/okm/downloadDocument?docTypemappingId={doc_id}&refId={ref_id}&refType={ref_type}&uuid={uuid}&version={version}"
            results['EIA Report PDF'] = eia_url

    return results

def main():
    json_files = recursive_find_json(directory)
    
    if not json_files:
        print(f"No JSON files found in {directory}")
        return
    
    print(f"Processing {len(json_files)} files...")
    
    # Use Polars to efficiently process the data
    data_list = []
    for file_path in json_files:
        result = parse_json(file_path)
        if result:  # Only add non-empty results
            data_list.append(result)
    
    if not data_list:
        print("No valid data found to process")
        return
    
    df = pl.DataFrame(data_list)
    
    # Filter rows to keep only those with a valid 'Proposal Number'
    if 'Proposal Number' in df.columns:
        df = df.filter(pl.col('Proposal Number').is_not_null())
    
    # Replace newlines with semicolons in string columns
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.replace_all("\n", ";"))
    
    # Reorder columns - put specified columns first, then remaining columns
    preferred_order = [
        'ID',
        'Proposal Type',
        'Proposal Number',
        'Application Date',
        'Grant Date',
        'Last Visible Status',
        
        'Project Land Requirement (Hectares)',
        'Total Cost (Lakhs)',
        
        'Organization Name',
        'EC Consultant',
        'Description',
        'Category',
        
        'Project Category (Code)',
        'Project Category',
    
        'Project Name',
        'State',
        'Sub District',
        'Village',

        'MoEFCC File',
        'State File',
        
        'Plot Nos',
        'Shape of Project',
        'Existing Non-Forest Land',
        'Existing Forest Land',
        'Existing Total Land',
        'Additional Non-Forest Land',
        'Additional Forest Land',
        'Additional Total Land',
        'Existing Cost',
        'Expansion Cost',
        'Villages Affected',
        'Project Displaced Families',
        'Project Affected Families',
        'Alternative Site Examined',
        'Alternative Site Description',
        'Government Restriction',
        'Litigation Pending',
        'Violation Involved',

        'Last Submission Date',
        
        'Project Exemption Reason',
        'EIA Report PDF',
    ]
    
    # Get columns that exist in the dataframe from the preferred order
    existing_preferred = [col for col in preferred_order if col in df.columns]
    
    # Get remaining columns that aren't in the preferred order
    remaining_cols = [col for col in df.columns if col not in preferred_order]
    
    # Combine to create final column order
    final_column_order = existing_preferred + remaining_cols
    
    # Reorder the dataframe columns
    df = df.select(final_column_order)
    
    # Sort by Application Date in descending order
    if 'Application Date' in df.columns:
        df = df.sort('Application Date', descending=True)
    
    # Ensure the output directory exists
    os.makedirs("csv", exist_ok=True)
    
    # Create output filename based on state parameter
    if state_param:
        output_file = f"csv/Projects_{state_param.upper()}.csv"
        print(f"Processing data for state: {state_param.upper()}")
    else:
        output_file = "csv/Projects.csv"
        print("Processing data for all states")
    
    # Write to CSV
    df.write_csv(output_file)
    print(f"Data saved to {output_file}")
    print(f"Total records: {len(df)}")

if __name__ == "__main__":
    main()
