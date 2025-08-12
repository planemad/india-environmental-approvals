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
            gdf = gpd.read_file(geojson_file)
            
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
                print(f"  Added {feature_count} features from {geojson_file}")
            else:
                print(f"  Warning: {geojson_file} is empty, skipping")
                
        except Exception as e:
            print(f"  Error reading {geojson_file}: {e}")
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
    output_path = "parivesh.gpkg"
    
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
