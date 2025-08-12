#!/usr/bin/env python3
"""
make_shape.py - Convert CSV environmental approvals data to GeoJSON with existing KML files
"""

import os
import sys
import csv
import json
import urllib.request
import urllib.parse
import ssl
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from xml.etree import ElementTree as ET

def download_kml_from_url(url: str, output_dir: Path, project_id: str = "", progress_info: str = "") -> tuple[Optional[Path], bool]:
    """Download KML file from URL and save to output directory"""
    try:
        # Create project-specific subdirectory if project_id is provided
        if project_id:
            output_dir = output_dir / project_id
        
        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename from URL parameters
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        
        # Use refId and uuid for unique filename
        ref_id = query_params.get('refId', ['unknown'])[0]
        uuid = query_params.get('uuid', ['unknown'])[0][:8]  # First 8 chars of UUID
        filename = f"{ref_id}_{uuid}.kml"
        
        output_path = output_dir / filename
        
        # Skip if file already exists
        if output_path.exists():
            if progress_info:
                print(f"  {progress_info} - Skipping existing: {filename}")
            return output_path, False  # False indicates no download occurred
            
        # Download the file
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(req, timeout=30, context=ssl_context) as response:
            content = response.read().decode('utf-8')
        
        # Check if response contains KML content
        if '<kml' in content.lower() or '<placemark' in content.lower():
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            if progress_info:
                print(f"  {progress_info} - Downloaded: {filename}")
            else:
                print(f"Downloaded KML: {filename}")
            return output_path, True  # True indicates download occurred
        else:
            print(f"Warning: URL did not return KML content: {url[:100]}...")
            return None, False
            
    except Exception as e:
        print(f"Error downloading KML from {url[:100]}...: {e}")
        return None, False

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
    """Main function to process CSV and create GeoJSON by downloading KML files from URLs"""
    
    # Use state-specific KML directory if state is provided
    if state:
        kml_dir = Path(f"kml/{state}")
    else:
        kml_dir = Path("kml")
    all_features = []
    
    # Read CSV file
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Count total rows for progress tracking
        rows = list(reader)
        total_projects = len(rows)
        
        processed_count = 0
        download_count = 0
        
        for row_idx, row in enumerate(rows, 1):
            proposal_id = row.get('Proposal Number', '')
            project_id = row.get('ID', '')
            kml_urls_str = row.get('KML URLs', '')
            
            if not kml_urls_str:
                print(f"No KML URLs found for {proposal_id}")
                continue
                
            # Parse multiple URLs separated by semicolon
            kml_urls = [url.strip() for url in kml_urls_str.split(';') if url.strip()]
            
            if not kml_urls:
                print(f"No valid KML URLs found for {proposal_id}")
                continue
                
            remaining_projects = total_projects - row_idx + 1
            print(f"Processing {proposal_id} (ID: {project_id}) ({row_idx}/{total_projects}, {remaining_projects} remaining) with {len(kml_urls)} KML URL(s)")
            
            # Download and process each KML file
            project_has_features = False
            for i, url in enumerate(kml_urls):
                progress_info = f"KML {i+1}/{len(kml_urls)} (Project {row_idx}/{total_projects})"
                
                # Download KML file
                kml_path, was_downloaded = download_kml_from_url(url, kml_dir, project_id, progress_info)
                
                if kml_path:
                    if was_downloaded:
                        download_count += 1
                        # Add small delay between actual downloads to be respectful
                        time.sleep(0.1)
                    
                    # Convert KML to GeoJSON features
                    features = kml_to_geojson_feature(kml_path, row)
                    if features:
                        all_features.extend(features)
                        project_has_features = True
                else:
                    print(f"  {progress_info} - Failed to download for {proposal_id}")
            
            if project_has_features:
                processed_count += 1
    
    print(f"Downloaded {download_count} KML files")
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