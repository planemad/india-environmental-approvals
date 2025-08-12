#!/usr/bin/env python3
"""
make_shape.py - Convert CSV environmental approvals data to GeoJSON with existing KML files
"""

import os
import sys
import csv
import json
import urllib.parse
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional
from xml.etree import ElementTree as ET

def generate_kml_filename(url: str) -> str:
    """Generate filename from KML URL parameters"""
    try:
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        
        # Use refId and uuid for unique filename
        ref_id = query_params.get('refId', ['unknown'])[0]
        uuid = query_params.get('uuid', ['unknown'])[0][:8]  # First 8 chars of UUID
        return f"{ref_id}_{uuid}.kml"
    except:
        # Fallback to hash of URL if parsing fails
        import hashlib
        return f"kml_{hashlib.md5(url.encode()).hexdigest()[:8]}.kml"

def generate_kml_url_file(csv_path: str, url_file_path: str, kml_dir: Path) -> int:
    """Generate URL file for batch downloading KML files"""
    url_count = 0
    
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        with open(url_file_path, 'w') as url_file:
            for row in reader:
                project_id = row.get('ID', '')
                kml_urls_str = row.get('KML URLs', '')
                
                if not kml_urls_str:
                    continue
                    
                # Parse multiple URLs separated by semicolon
                kml_urls = [url.strip() for url in kml_urls_str.split(';') if url.strip()]
                
                for url in kml_urls:
                    # Create project-specific output path
                    filename = generate_kml_filename(url)
                    output_path = kml_dir / project_id / filename
                    
                    # Write URL and output path to file (tab-separated)
                    url_file.write(f"{url}\t{output_path}\n")
                    url_count += 1
    
    return url_count

def batch_download_kmls(url_file_path: str) -> bool:
    """Use request.py to batch download KML files"""
    try:
        # Call request.py with appropriate parameters for KML downloads
        cmd = [
            'python3', 'request.py', url_file_path,
            '--content-type', 'kml',
            '--http-method', 'GET',
            '--min-batch-size', '5',
            '--max-batch-size', '15',
            '--min-delay', '1.0', 
            '--max-delay', '3.0',
            '--max-concurrent', '8'
        ]
        
        result = subprocess.run(cmd, capture_output=False, text=True)
        return result.returncode == 0
        
    except Exception as e:
        print(f"Error running batch downloader: {e}")
        return False

def parse_kml_coordinates(coord_string: str) -> List[List[float]]:
    """Parse KML coordinate string into list of [lon, lat] pairs"""
    coordinates = []
    
    # Clean up the coordinate string
    coord_string = coord_string.strip()
    
    # Split by whitespace and/or commas
    coord_parts = coord_string.replace(',', ' ').split()
    
    # Group coordinates by 3 (lon, lat, alt) or 2 (lon, lat)
    i = 0
    while i < len(coord_parts):
        try:
            lon = float(coord_parts[i])
            lat = float(coord_parts[i + 1])
            coordinates.append([lon, lat])
            
            # Skip altitude if present
            if i + 2 < len(coord_parts) and coord_parts[i + 2].replace('.', '').replace('-', '').isdigit():
                i += 3
            else:
                i += 2
                
        except (ValueError, IndexError):
            i += 1
            
    return coordinates

def kml_to_geojson_feature(kml_path: Path, csv_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert KML file to GeoJSON features with CSV attributes"""
    features = []
    
    try:
        with open(kml_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(kml_path, 'r', encoding='latin-1') as f:
                content = f.read()
        except:
            print(f"Could not read {kml_path}")
            return features
    
    try:
        root = ET.fromstring(content)
        
        # Handle namespace
        ns = {'kml': 'http://www.opengis.net/kml/2.2'}
        if root.tag.startswith('{'):
            ns_uri = root.tag.split('}')[0][1:]
            ns = {'kml': ns_uri}
        
        # Find all placemarks
        placemarks = root.findall('.//kml:Placemark', ns)
        if not placemarks:
            placemarks = root.findall('.//Placemark')
        
        for placemark in placemarks:
            feature = {
                "type": "Feature",
                "properties": dict(csv_row),  # Copy all CSV attributes
                "geometry": None
            }
            
            # Add placemark name if available
            name_elem = placemark.find('.//kml:name', ns)
            if name_elem is None:
                name_elem = placemark.find('.//name')
            if name_elem is not None and name_elem.text:
                feature["properties"]["kml_name"] = name_elem.text
            
            # Add placemark description if available
            desc_elem = placemark.find('.//kml:description', ns)
            if desc_elem is None:
                desc_elem = placemark.find('.//description')
            if desc_elem is not None and desc_elem.text:
                feature["properties"]["kml_description"] = desc_elem.text
            
            # Handle different geometry types
            
            # Point
            point = placemark.find('.//kml:Point/kml:coordinates', ns)
            if point is None:
                point = placemark.find('.//Point/coordinates')
            if point is not None and point.text:
                coords = parse_kml_coordinates(point.text)
                if coords:
                    feature["geometry"] = {
                        "type": "Point",
                        "coordinates": coords[0]
                    }
            
            # LineString
            linestring = placemark.find('.//kml:LineString/kml:coordinates', ns)
            if linestring is None:
                linestring = placemark.find('.//LineString/coordinates')
            if linestring is not None and linestring.text:
                coords = parse_kml_coordinates(linestring.text)
                if coords:
                    feature["geometry"] = {
                        "type": "LineString",
                        "coordinates": coords
                    }
            
            # Polygon
            polygon = placemark.find('.//kml:Polygon', ns)
            if polygon is None:
                polygon = placemark.find('.//Polygon')
            if polygon is not None:
                # Outer boundary
                outer_coords = polygon.find('.//kml:outerBoundaryIs/kml:LinearRing/kml:coordinates', ns)
                if outer_coords is None:
                    outer_coords = polygon.find('.//outerBoundaryIs/LinearRing/coordinates')
                
                if outer_coords is not None and outer_coords.text:
                    coords = parse_kml_coordinates(outer_coords.text)
                    if coords:
                        # Close the polygon if not already closed
                        if coords[0] != coords[-1]:
                            coords.append(coords[0])
                        
                        feature["geometry"] = {
                            "type": "Polygon",
                            "coordinates": [coords]
                        }
                        
                        # Handle inner boundaries (holes)
                        inner_boundaries = polygon.findall('.//kml:innerBoundaryIs/kml:LinearRing/kml:coordinates', ns)
                        if not inner_boundaries:
                            inner_boundaries = polygon.findall('.//innerBoundaryIs/LinearRing/coordinates')
                        
                        for inner in inner_boundaries:
                            if inner.text:
                                inner_coords = parse_kml_coordinates(inner.text)
                                if inner_coords:
                                    if inner_coords[0] != inner_coords[-1]:
                                        inner_coords.append(inner_coords[0])
                                    feature["geometry"]["coordinates"].append(inner_coords)
            
            # Only add features with valid geometry
            if feature["geometry"] is not None:
                features.append(feature)
        
    except ET.ParseError as e:
        print(f"Error parsing KML {kml_path}: {e}")
    except Exception as e:
        print(f"Unexpected error processing KML {kml_path}: {e}")
    
    return features

def process_csv_to_geojson(csv_path: str, output_path: str = "geojsonoutput.geojson", state: str = ""):
    """Main function to process CSV and create GeoJSON by batch downloading KML files"""
    
    # Use state-specific KML directory if state is provided
    if state:
        kml_dir = Path(f"kml/{state}")
    else:
        kml_dir = Path("kml")
    
    # Generate URL file for batch downloading
    url_file_path = f"kml_urls_{state}.txt" if state else "kml_urls_all.txt"
    
    print("Generating KML URL list for batch downloading...")
    url_count = generate_kml_url_file(csv_path, url_file_path, kml_dir)
    print(f"Generated {url_count} KML URLs")
    
    if url_count == 0:
        print("No KML URLs found in CSV file")
        return
    
    # Batch download KML files using request.py
    print("\nStarting batch download of KML files...")
    download_success = batch_download_kmls(url_file_path)
    
    if not download_success:
        print("Warning: Batch download encountered errors. Continuing with available files...")
    
    # Clean up URL file
    try:
        os.remove(url_file_path)
    except OSError:
        pass
    
    # Process downloaded KML files into GeoJSON
    print("\nProcessing KML files to GeoJSON...")
    all_features = []
    
    # Read CSV file again to process each row
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        total_projects = len(rows)
        processed_count = 0
        
        for row_idx, row in enumerate(rows, 1):
            proposal_id = row.get('Proposal Number', '')
            project_id = row.get('ID', '')
            kml_urls_str = row.get('KML URLs', '')
            
            if not kml_urls_str:
                continue
                
            # Parse multiple URLs separated by semicolon
            kml_urls = [url.strip() for url in kml_urls_str.split(';') if url.strip()]
            
            if not kml_urls:
                continue
            
            print(f"Processing {proposal_id} (ID: {project_id}) ({row_idx}/{total_projects}) with {len(kml_urls)} KML file(s)")
            
            # Process each KML file for this project
            project_has_features = False
            for url in kml_urls:
                # Generate the expected file path
                filename = generate_kml_filename(url)
                kml_path = kml_dir / project_id / filename
                
                if kml_path.exists():
                    # Convert KML to GeoJSON features
                    features = kml_to_geojson_feature(kml_path, row)
                    if features:
                        all_features.extend(features)
                        project_has_features = True
                else:
                    print(f"  Warning: KML file not found: {kml_path}")
            
            if project_has_features:
                processed_count += 1
    
    print(f"Processed {processed_count} projects with valid geometry")
    
    # Create GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "features": all_features
    }
    
    # Write GeoJSON file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)
    
    print(f"Created {output_path} with {len(all_features)} features")

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python 4_make_shape.py <STATE> [output_file]")
        print("Example: python 4_make_shape.py 30")
        print("This will read csv/Projects_30.csv and output to geojson/Projects_30.geojson")
        sys.exit(1)
    
    state = sys.argv[1]
    
    # Generate input and output paths based on state
    csv_path = f"csv/Projects_{state}.csv"
    
    if len(sys.argv) > 2:
        output_path = sys.argv[2]
    else:
        # Create geojson directory if it doesn't exist
        os.makedirs("geojson", exist_ok=True)
        output_path = f"geojson/Projects_{state}.geojson"
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found")
        sys.exit(1)
    
    print(f"Processing state {state}")
    print(f"Input: {csv_path}")
    print(f"Output: {output_path}")
    print(f"KML files will be saved to: kml/{state}/$ID/")
    print()
    
    process_csv_to_geojson(csv_path, output_path, state)

if __name__ == "__main__":
    main()