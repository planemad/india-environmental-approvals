#!/usr/bin/env python3
"""
5_combine_geojson.py - Combine all GeoJSON files into a single GeoPackage
"""

import os
import sys
import glob
from pathlib import Path
import pandas as pd
import geopandas as gpd
from typing import List

def find_geojson_files(geojson_dir: str = "geojson") -> List[str]:
    """Find all GeoJSON files in the specified directory"""
    geojson_pattern = os.path.join(geojson_dir, "*.geojson")
    geojson_files = glob.glob(geojson_pattern)
    return sorted(geojson_files)

def combine_geojson_to_gpkg(geojson_files: List[str], output_path: str = "india-environmental-approvals.gpkg"):
    """Combine multiple GeoJSON files into a single GeoPackage"""
    
    if not geojson_files:
        print("No GeoJSON files found to combine.")
        return
    
    print(f"Found {len(geojson_files)} GeoJSON files to combine:")
    for geojson_file in geojson_files:
        print(f"  - {geojson_file}")
    
    combined_gdfs = []
    total_features = 0
    
    for geojson_file in geojson_files:
        try:
            print(f"Reading {geojson_file}...")
            # Try to read with GeoPandas first
            try:
                gdf = gpd.read_file(geojson_file)
                
                if not gdf.empty:
                    # Validate and clean geometries
                    initial_count = len(gdf)
                    
                    # Remove invalid geometries
                    gdf = gdf[gdf.geometry.is_valid & ~gdf.geometry.is_empty]
                    
                    # Remove rows with null geometries
                    gdf = gdf[gdf.geometry.notnull()]
                    
                    if len(gdf) < initial_count:
                        invalid_count = initial_count - len(gdf)
                        print(f"  Warning: Removed {invalid_count} invalid/empty geometries")
                    
                    if not gdf.empty:
                        # Add source file information
                        source_filename = os.path.basename(geojson_file)
                        gdf['source_file'] = source_filename
                        
                        # Extract state code from filename if it follows the pattern Projects_XX.geojson
                        if source_filename.startswith('Projects_') and source_filename.endswith('.geojson'):
                            state_code = source_filename.replace('Projects_', '').replace('.geojson', '')
                            gdf['state_code'] = state_code
                        
                        combined_gdfs.append(gdf)
                        feature_count = len(gdf)
                        total_features += feature_count
                        print(f"  Added {feature_count} valid features from {geojson_file}")
                    else:
                        print(f"  Warning: No valid geometries found in {geojson_file}, skipping")
                else:
                    print(f"  Warning: {geojson_file} is empty, skipping")
                    
            except Exception as geopandas_error:
                print(f"  GeoPandas error reading {geojson_file}: {geopandas_error}")
                print(f"  Attempting to process with geometry validation...")
                
                # Try to read as raw JSON and validate manually
                try:
                    import json
                    with open(geojson_file, 'r') as f:
                        geojson_data = json.load(f)
                    
                    # Validate and clean features
                    valid_features = []
                    invalid_count = 0
                    
                    for feature in geojson_data.get('features', []):
                        geometry = feature.get('geometry', {})
                        geom_type = geometry.get('type')
                        coordinates = geometry.get('coordinates', [])
                        
                        # Basic validation for common geometry types
                        is_valid = False
                        try:
                            if geom_type == 'Point' and isinstance(coordinates, list) and len(coordinates) == 2:
                                # Ensure coordinates are valid numbers
                                if all(isinstance(coord, (int, float)) for coord in coordinates):
                                    is_valid = True
                            elif geom_type == 'LineString' and isinstance(coordinates, list) and len(coordinates) >= 2:
                                # Ensure all coordinates are valid point arrays
                                if all(isinstance(point, list) and len(point) == 2 and 
                                      all(isinstance(coord, (int, float)) for coord in point) for point in coordinates):
                                    is_valid = True
                            elif geom_type == 'Polygon' and isinstance(coordinates, list) and len(coordinates) >= 1:
                                # Check if outer ring has at least 4 coordinates and all are valid
                                if (isinstance(coordinates[0], list) and len(coordinates[0]) >= 4 and
                                    all(isinstance(point, list) and len(point) == 2 and 
                                        all(isinstance(coord, (int, float)) for coord in point) for point in coordinates[0])):
                                    is_valid = True
                            elif geom_type == 'MultiPoint' and isinstance(coordinates, list) and len(coordinates) > 0:
                                # Validate each point in the MultiPoint
                                if all(isinstance(point, list) and len(point) == 2 and 
                                      all(isinstance(coord, (int, float)) for coord in point) for point in coordinates):
                                    is_valid = True
                            elif geom_type == 'MultiLineString' and isinstance(coordinates, list) and len(coordinates) > 0:
                                # Validate each LineString in the MultiLineString
                                if all(isinstance(line, list) and len(line) >= 2 and
                                      all(isinstance(point, list) and len(point) == 2 and 
                                          all(isinstance(coord, (int, float)) for coord in point) for point in line) 
                                      for line in coordinates):
                                    is_valid = True
                            elif geom_type == 'MultiPolygon' and isinstance(coordinates, list) and len(coordinates) > 0:
                                # Validate each Polygon in the MultiPolygon (basic check)
                                if all(isinstance(polygon, list) and len(polygon) >= 1 for polygon in coordinates):
                                    is_valid = True
                        except (TypeError, IndexError, ValueError):
                            # If any error occurs during validation, mark as invalid
                            is_valid = False
                        
                        if is_valid:
                            valid_features.append(feature)
                        else:
                            invalid_count += 1
                    
                    if valid_features:
                        print(f"  Found {len(valid_features)} valid features after manual validation")
                        if invalid_count > 0:
                            print(f"  Removed {invalid_count} invalid geometries")
                        
                        # Create a new GeoJSON structure with valid features
                        cleaned_geojson = {
                            "type": "FeatureCollection",
                            "features": valid_features
                        }
                        
                        # Write to a temporary file and read with GeoPandas
                        temp_file = geojson_file + '.temp'
                        with open(temp_file, 'w') as f:
                            json.dump(cleaned_geojson, f)
                        
                        try:
                            gdf = gpd.read_file(temp_file)
                            os.remove(temp_file)  # Clean up temp file
                            
                            if not gdf.empty:
                                # Add source file information
                                source_filename = os.path.basename(geojson_file)
                                gdf['source_file'] = source_filename
                                
                                # Extract state code from filename if it follows the pattern Projects_XX.geojson
                                if source_filename.startswith('Projects_') and source_filename.endswith('.geojson'):
                                    state_code = source_filename.replace('Projects_', '').replace('.geojson', '')
                                    gdf['state_code'] = state_code
                                
                                combined_gdfs.append(gdf)
                                feature_count = len(gdf)
                                total_features += feature_count
                                print(f"  Added {feature_count} features after cleaning from {geojson_file}")
                            else:
                                print(f"  Warning: No valid features after cleaning {geojson_file}")
                                
                        except Exception as temp_error:
                            print(f"  Error reading cleaned temporary file: {temp_error}")
                            if os.path.exists(temp_file):
                                os.remove(temp_file)
                            continue
                    else:
                        print(f"  No valid features found in {geojson_file}")
                        
                except json.JSONDecodeError as json_error:
                    print(f"  JSON parsing error: {json_error}")
                    continue
                except Exception as manual_error:
                    print(f"  Manual validation error: {manual_error}")
                    continue
                
        except Exception as e:
            print(f"  Unexpected error reading {geojson_file}: {e}")
            continue
    
    if not combined_gdfs:
        print("No valid GeoJSON data found to combine.")
        return
    
    print(f"\nCombining {len(combined_gdfs)} GeoDataFrames...")
    
    # Combine all GeoDataFrames
    try:
        combined_gdf = gpd.GeoDataFrame(pd.concat(combined_gdfs, ignore_index=True))
        
        # Ensure the CRS is set (default to WGS84 if not specified)
        if combined_gdf.crs is None:
            combined_gdf.set_crs('EPSG:4326', inplace=True)
            print("Set CRS to WGS84 (EPSG:4326)")
        
        print(f"Combined dataset has {len(combined_gdf)} total features")
        print(f"CRS: {combined_gdf.crs}")
        print(f"Columns: {list(combined_gdf.columns)}")
        
        # Write to GeoPackage
        print(f"\nWriting to {output_path}...")
        combined_gdf.to_file(output_path, driver='GPKG')
        
        print(f"Successfully created {output_path}")
        print(f"Total features: {len(combined_gdf)}")
        
        # Print summary statistics
        if 'state_code' in combined_gdf.columns:
            state_counts = combined_gdf['state_code'].value_counts()
            print(f"\nFeatures by state:")
            for state, count in state_counts.items():
                print(f"  State {state}: {count} features")
        
        # Print geometry type distribution
        geom_types = combined_gdf.geometry.geom_type.value_counts()
        print(f"\nGeometry types:")
        for geom_type, count in geom_types.items():
            print(f"  {geom_type}: {count}")
            
    except Exception as e:
        print(f"Error combining GeoDataFrames: {e}")
        return

def main():
    """Main entry point"""
    
    # Check if geopandas is available
    try:
        import geopandas as gpd
        import pandas as pd
    except ImportError as e:
        print("Error: Required packages not found.")
        print("Please install required packages:")
        print("  pip install geopandas pandas")
        print(f"Missing: {e}")
        sys.exit(1)
    
    # Set up paths
    geojson_dir = "geojson"
    output_path = "india-environmental-approvals.gpkg"
    
    # Allow custom output path as command line argument
    if len(sys.argv) > 1:
        output_path = sys.argv[1]
    
    print("Parivesh GeoJSON to GeoPackage Combiner")
    print("=" * 40)
    print(f"Looking for GeoJSON files in: {geojson_dir}/")
    print(f"Output file: {output_path}")
    print()
    
    # Check if geojson directory exists
    if not os.path.exists(geojson_dir):
        print(f"Error: Directory '{geojson_dir}' not found")
        print("Please run the make_shape.py script first to generate GeoJSON files")
        sys.exit(1)
    
    # Find GeoJSON files
    geojson_files = find_geojson_files(geojson_dir)
    
    if not geojson_files:
        print(f"No GeoJSON files found in {geojson_dir}/")
        print("Please run the make_shape.py script first to generate GeoJSON files")
        sys.exit(1)
    
    # Combine files
    combine_geojson_to_gpkg(geojson_files, output_path)

if __name__ == "__main__":
    main()
