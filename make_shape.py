#!/usr/bin/env python3
"""
make_shape.py - Convert CSV environmental approvals data to GeoJSON with existing KML files
"""

import os
import sys
import csv
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from xml.etree import ElementTree as ET

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

def process_csv_to_geojson(csv_path: str, output_path: str = "geojsonoutput.geojson"):
    """Main function to process CSV and create GeoJSON using existing KML files"""
    
    kml_dir = Path("kml")
    all_features = []
    
    # Create a mapping of proposal numbers to KML files
    kml_files = {}
    for kml_file in kml_dir.glob("*.kml"):
        # Extract proposal number from filename
        # Example: SIA_GA_MIN_442457_2023.kml -> SIA/GA/MIN/442457/2023
        parts = kml_file.stem.split('_')
        if len(parts) >= 5:
            proposal_parts = parts[:-1]  # Remove year
            year = parts[-1]
            # Reconstruct proposal number
            proposal_id = '/'.join(proposal_parts) + '/' + year
            kml_files[proposal_id] = kml_file
    
    print(f"Found {len(kml_files)} KML files")
    
    # Read CSV file
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        processed_count = 0
        for row in reader:
            proposal_id = row.get('Proposal Number', '')
            
            if proposal_id in kml_files:
                kml_path = kml_files[proposal_id]
                print(f"Processing {proposal_id} -> {kml_path.name}")
                
                # Convert KML to GeoJSON features
                features = kml_to_geojson_feature(kml_path, row)
                all_features.extend(features)
                processed_count += 1
            else:
                print(f"No KML file found for {proposal_id}")
    
    print(f"Processed {processed_count} projects")
    
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
        print("Usage: python make_shape_fixed.py <csv_file> [output_file]")
        print("Example: python make_shape_fixed.py csv/Projects_30.csv output.geojson")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output.geojson"
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found")
        sys.exit(1)
    
    process_csv_to_geojson(csv_path, output_path)

if __name__ == "__main__":
    main()