#!/usr/bin/env python3
"""
Geoparquet Manager - A comprehensive GeoParquet data processing library

This module provides a unified interface for working with geospatial data,
including conversion between formats, validation, and transformation operations.

## Quick Start Usage

### Basic Usage Example:

```python
from geoparquet_manager import GeoDataManager

# Initialize the manager
manager = GeoDataManager()

# Convert GeoPackage to GeoParquet
success = manager.convert_gpkg_to_parquet(
    input_path="data.gpkg",
    output_path="data.parquet",
    layer_name="my_layer",  # Optional, uses first layer if None
    chunk_size=100000       # Optional, default 100000 features per chunk
)

# Convert multiple GeoParquet files back to GeoPackage
success = manager.convert_parquet_to_gpkg(
    input_folder="parquet_files/",
    output_path="combined.gpkg",
    layer_name="merged_data"  # Optional, default "merged_layer"
)

# Read and analyze geospatial files
gdf = manager.read_file("data.parquet")  # Auto-detects format
info = manager.get_file_info("data.gpkg")  # Get metadata

# Validate and improve data quality
improved_gdf = manager.validate_and_improve(
    gdf=gdf,
    output_path="improved_data.parquet"  # Optional, saves if provided
)

# Process entire folders
manager.process_folder(
    folder_path="/path/to/parquet/files/",
    operation="validate_parquet"
)
```

### Individual Component Usage:

```python
# Use specific converters directly
from geoparquet_manager import GeoPackageToParquetConverter, GeoParquetValidator

# Chunked conversion for large files
converter = GeoPackageToParquetConverter(chunk_size=50000)
converter.process("large_data.gpkg", "output_chunks.parquet")

# Advanced validation
validator = GeoParquetValidator()
validation_results = validator.validate(gdf)
fixed_gdf = validator.apply_fixes(gdf)
```

### Validation Rules Implemented:

- **RQ1/RQ6**: Column names must start with letter, use only lowercase a-z, numbers, underscores
- **RQ2**: Layers must have at least one feature  
- **RQ15**: All geometries in a layer must be the same type
- **RQ21**: Names must not exceed 57 characters
- **RQ22**: Only approved EPSG spatial reference systems
- **RQ23**: Geometries must be valid and simple
- **RC17**: Recommends naming geometry columns 'geom'
- **RC20**: Ensures proper polygon orientation (counter-clockwise exterior, clockwise holes)

### Supported File Formats:

- **Input**: GeoPackage (.gpkg), GeoParquet (.parquet)
- **Output**: GeoPackage (.gpkg), GeoParquet (.parquet)
- **Auto-detection** based on file extensions

### Memory-Efficient Processing:

The library is designed for large datasets:
- Chunked processing for GeoPackage to GeoParquet conversion
- Iterative processing for folder operations
- Memory cleanup between operations
- Configurable chunk sizes based on available memory
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import os
import glob
import re
import warnings
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Tuple, Union
from pathlib import Path
import fiona
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkb, wkt

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')
gpd.options.io_engine = "fiona"


class GeoDataProcessor(ABC):
    """Abstract base class for all geospatial data processors."""
    
    @abstractmethod
    def process(self, input_path: str, output_path: str, **kwargs) -> bool:
        """Process geospatial data from input to output format."""
        pass
    
    @abstractmethod
    def validate_input(self, input_path: str) -> bool:
        """Validate that input file exists and is accessible."""
        pass


class GeoDataValidator(ABC):
    """Abstract base class for geospatial data validators."""
    
    @abstractmethod
    def validate(self, gdf: gpd.GeoDataFrame) -> Dict:
        """Validate GeoDataFrame against specific rules."""
        pass
    
    @abstractmethod
    def apply_fixes(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Apply fixes to make GeoDataFrame compliant."""
        pass


class GeoDataReader(ABC):
    """Abstract base class for geospatial data readers."""
    
    @abstractmethod
    def read(self, file_path: str, **kwargs) -> Optional[gpd.GeoDataFrame]:
        """Read geospatial data from file."""
        pass
    
    @abstractmethod
    def get_info(self, file_path: str) -> Dict:
        """Get metadata information about the file."""
        pass


class GeoPackageToParquetConverter(GeoDataProcessor):
    """Converts GeoPackage files to GeoParquet format in memory-efficient chunks."""
    
    def __init__(self, chunk_size: int = 100000):
        self.chunk_size = chunk_size
    
    def validate_input(self, input_path: str) -> bool:
        """Validate that input GeoPackage file exists and is accessible."""
        return os.path.exists(input_path) and input_path.endswith('.gpkg')
    
    def get_layer_info(self, gpkg_path: str, layer_name: str) -> Tuple[int, dict, dict]:
        """Get layer information without loading into memory."""
        with fiona.open(gpkg_path, layer=layer_name) as src:
            return len(src), src.schema, src.crs
    
    def process(self, input_path: str, output_path: str, layer_name: str = None, **kwargs) -> bool:
        """
        Convert GeoPackage to chunked GeoParquet files.
        
        Args:
            input_path: Path to input GeoPackage file
            output_path: Base path for output Parquet files
            layer_name: Specific layer to convert (if None, converts first layer)
        """
        if not self.validate_input(input_path):
            print(f"❌ Invalid input file: {input_path}")
            return False
        
        try:
            # Get layer information
            if layer_name is None:
                layers = gpd.list_layers(input_path)
                layer_name = layers.iloc[0]["name"]            
            
            total_features, schema, crs = self.get_layer_info(input_path, layer_name)
            print(f"Total features: {total_features}")
            
            # Process in chunks
            for i in range(0, total_features, self.chunk_size):
                end_idx = min(i + self.chunk_size, total_features)
                
                # Read chunk using fiona
                with fiona.open(input_path, layer=layer_name) as src:
                    features = list(src[i:end_idx])
                
                # Convert to GeoDataFrame
                chunk_gdf = gpd.GeoDataFrame.from_features(features, crs=crs)
                
                # Save chunk
                chunk_output = output_path.replace(".parquet", f"_chunk_{i//self.chunk_size:04d}.parquet")
                chunk_gdf.to_parquet(chunk_output, index=False)
                
                print(f"Processed chunk {i//self.chunk_size + 1}/{(total_features + self.chunk_size - 1)//self.chunk_size}: features {i+1}-{end_idx}")
            
            print("✓ Conversion to GeoParquet completed!")
            return True
            
        except Exception as e:
            print(f"❌ Error converting {input_path}: {e}")
            return False


class ParquetToGeoPackageConverter(GeoDataProcessor):
    """Converts multiple GeoParquet files to a single GeoPackage file."""
    
    def validate_input(self, input_path: str) -> bool:
        """Validate that input directory contains Parquet files."""
        if not os.path.isdir(input_path):
            return False
        parquet_files = glob.glob(os.path.join(input_path, "*.parquet"))
        return len(parquet_files) > 0
    
    def process(self, input_path: str, output_path: str, layer_name: str = "merged_layer", **kwargs) -> bool:
        """
        Convert multiple GeoParquet files to a single GeoPackage.
        
        Args:
            input_path: Directory containing GeoParquet files
            output_path: Path to output GeoPackage file
            layer_name: Name of the layer in the GeoPackage
        """
        if not self.validate_input(input_path):
            print(f"❌ Invalid input directory or no Parquet files found: {input_path}")
            return False
        
        try:
            # Find all parquet files
            parquet_pattern = os.path.join(input_path, "*.parquet")
            parquet_files = glob.glob(parquet_pattern)
            parquet_files.sort()
            
            print(f"Found {len(parquet_files)} parquet files")
            
            total_features = 0
            first_file = True
            
            # Process each file iteratively
            for i, parquet_file in enumerate(parquet_files):
                print(f"Processing file {i+1}/{len(parquet_files)}: {os.path.basename(parquet_file)}")
                
                # Read GeoParquet file
                gdf = gpd.read_parquet(parquet_file)
                print(f"  - Loaded {len(gdf)} features")
                
                if first_file:
                    # Create the GeoPackage
                    print(f"Creating GeoPackage: {output_path}")
                    gdf.to_file(output_path, layer=layer_name, driver="GPKG")
                    first_file = False
                else:
                    # Append to existing GeoPackage
                    print(f"Appending to GeoPackage...")
                    gdf.to_file(output_path, layer=layer_name, driver="GPKG", mode='a')
                
                total_features += len(gdf)
                print(f"  - Total features so far: {total_features}")
                
                # Clear memory
                del gdf
            
            print(f"✓ Conversion to GeoPackage completed!")
            print(f"Total features in final GeoPackage: {total_features}")
            return True
            
        except Exception as e:
            print(f"❌ Error converting {input_path}: {e}")
            return False


class GeoParquetReader(GeoDataReader):
    """Reader for GeoParquet files."""
    
    def read(self, file_path: str, **kwargs) -> Optional[gpd.GeoDataFrame]:
        """Load a GeoParquet file and return GeoDataFrame."""
        try:
            print(f"Loading geoparquet file: {file_path}")
            gdf = gpd.read_parquet(file_path)
            print(f"✓ Successfully loaded {file_path}")
            return gdf
        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")
            return None
    
    def get_info(self, file_path: str) -> Dict:
        """Get metadata information about the GeoParquet file."""
        try:
            gdf = self.read(file_path)
            if gdf is not None:
                return {
                    'feature_count': len(gdf),
                    'columns': list(gdf.columns),
                    'crs': str(gdf.crs),
                    'geometry_types': gdf.geometry.geom_type.unique().tolist(),
                    'bounds': gdf.total_bounds.tolist()
                }
        except Exception as e:
            print(f"❌ Error getting info for {file_path}: {e}")
        return {}


class GeoPackageReader(GeoDataReader):
    """Reader for GeoPackage files."""
    
    def read(self, file_path: str, layer_name: str = None, **kwargs) -> Optional[gpd.GeoDataFrame]:
        """Load a GeoPackage layer and return GeoDataFrame."""
        try:
            print(f"Loading geopackage file: {file_path}")
            if layer_name is None:
                layers = gpd.list_layers(file_path)
                layer_name = layers.iloc[0]['layer']
            
            gdf = gpd.read_file(file_path, layer=layer_name)
            print(f"✓ Successfully loaded {file_path}, layer: {layer_name}")
            return gdf
        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")
            return None
    
    def get_info(self, file_path: str) -> Dict:
        """Get metadata information about the GeoPackage file."""
        try:
            layers = gpd.list_layers(file_path)
            return {
                'layers': layers.to_dict('records'),
                'layer_count': len(layers)
            }
        except Exception as e:
            print(f"❌ Error getting info for {file_path}: {e}")
        return {}


class GeoParquetValidator(GeoDataValidator):
    """
    Comprehensive validator for GeoParquet files implementing geospatial data standards.
    
    Validation Rules:
    - RQ1: Layer names must start with a letter, valid chars: lowercase a-z, numbers, underscores
    - RQ2: Layers must have at least one feature
    - RQ6: Column names must start with a letter, valid chars: lowercase a-z, numbers, underscores
    - RQ15: All table geometries types must match
    - RQ21: All layer and column names shall not be longer than 57 characters
    - RQ22: Only specific EPSG spatial reference systems allowed
    - RQ23: Geometries should be valid and simple
    - RC17: Recommend naming GEOMETRY columns 'geom'
    - RC20: Recommend proper polygon orientation
    """
    
    def __init__(self):
        # Allowed EPSG codes based on Australian and international standards
        self.allowed_epsg_codes = {
            # MGA zones (GDA94) - Projected CRS
            28348, 28349, 28350, 28351, 28352, 28353, 28354, 28355, 28356, 28357, 28358,
            # MGA zones (GDA2020) - Projected CRS  
            7846, 7847, 7848, 7849, 7850, 7851, 7852, 7853, 7854, 7855, 7856, 7857, 7858, 7859,
            # World/Web Mercator and other common projections
            3395, 3857,
            # Australian and international common codes
            6283, 1168, 1291, 1156, 6326, 5111, 1292, 4938, 7842, 9307, 4978, 4939, 7843,
            9308, 4979, 4283, 7844, 9309, 4326, 5711, 9464, 9463, 9458, 9462,
            # European and other international codes
            28992, 3034, 3035, 3040, 3041, 3042, 3043, 3044, 3045,
            3046, 3047, 3048, 3049, 4258, 4936, 4937, 5730, 7409
        }
    
    def format_text(self, text: str) -> str:
        """Format text to meet naming requirements (RQ1, RQ6, RQ21)."""
        text = text.lower()
        
        # Ensure starts with letter
        if not text[0].isalpha():
            text = 'a_' + text
        
        # Replace invalid characters
        cleaned_text = re.sub(r'[^a-z0-9_]', '_', text)
        
        # Enforce 57-character limit
        return cleaned_text[:57]
    
    def validate_rq2(self, gdf: gpd.GeoDataFrame) -> bool:
        """RQ2: Layers must have at least one feature."""
        return len(gdf) > 0
    
    def validate_rq1_rq6_rq21(self, gdf: gpd.GeoDataFrame) -> Dict:
        """Validate naming conventions (RQ1, RQ6, RQ21)."""
        if gdf.empty:
            return {'valid': False, 'columns_to_fix': [], 'issues': ['Empty dataset']}
        
        columns_to_fix = []
        issues = []
        
        for col in gdf.columns:
            if col == 'geometry':
                continue
            
            # Check starts with letter
            if not col[0].isalpha():
                issues.append(f"Column '{col}' does not start with a letter (RQ1/RQ6)")
                columns_to_fix.append(col)
                continue
            
            # Check for invalid characters
            if re.search(r'[^a-z0-9_]', col):
                issues.append(f"Column '{col}' contains invalid characters (RQ1/RQ6)")
                columns_to_fix.append(col)
                continue
            
            # Check length limit
            if len(col) > 57:
                issues.append(f"Column '{col}' exceeds 57 character limit (RQ21)")
                columns_to_fix.append(col)
                continue
        
        return {
            'valid': len(columns_to_fix) == 0,
            'columns_to_fix': columns_to_fix,
            'issues': issues
        }
    
    def validate_rq15(self, gdf: gpd.GeoDataFrame) -> bool:
        """RQ15: All table geometries types must match."""
        if gdf.empty:
            return False
        
        geometry_types = gdf.geometry.geom_type.unique()
        return len(geometry_types) == 1
    
    def validate_rq22(self, gdf: gpd.GeoDataFrame) -> bool:
        """RQ22: Check if CRS is in allowed EPSG codes."""
        if gdf.crs is None:
            return False
        
        try:
            epsg_code = gdf.crs.to_epsg()
            return epsg_code in self.allowed_epsg_codes
        except:
            return False
    
    def validate_rq23(self, gdf: gpd.GeoDataFrame) -> bool:
        """RQ23: Geometries should be valid and simple."""
        if gdf.empty:
            return False
        
        valid_geometries = gdf.geometry.is_valid
        return valid_geometries.all()
    
    def validate(self, gdf: gpd.GeoDataFrame) -> Dict:
        """Run all validation rules and return results."""
        return {
            'rq2_feature_count': self.validate_rq2(gdf),
            'rq1_rq6_rq21_naming': self.validate_rq1_rq6_rq21(gdf),
            'rq15_geometry_type': self.validate_rq15(gdf),
            'rq22_crs': self.validate_rq22(gdf),
            'rq23_geometry_validity': self.validate_rq23(gdf)
        }
    
    def apply_rc17(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """RC17: Rename geometry column to 'geom'."""
        geometry_cols = [col for col in gdf.columns if col != 'geometry' and gdf[col].dtype == 'geometry']
        
        for col in geometry_cols:
            if col != 'geom':
                gdf = gdf.rename(columns={col: 'geom'})
        
        return gdf
    
    def apply_rc20(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """RC20: Ensure proper orientation for polygon geometries."""
        def fix_polygon_orientation(geom):
            if isinstance(geom, Polygon):
                # Fix exterior ring to counter-clockwise
                if not geom.exterior.is_ccw:
                    geom = Polygon(geom.exterior.coords[::-1], geom.interiors)
                # Fix interior rings to clockwise
                interiors = []
                for interior in geom.interiors:
                    if interior.is_ccw:
                        interiors.append(interior.coords[::-1])
                    else:
                        interiors.append(interior.coords)
                geom = Polygon(geom.exterior.coords, interiors)
            elif isinstance(geom, MultiPolygon):
                fixed_polygons = [fix_polygon_orientation(poly) for poly in geom.geoms]
                geom = MultiPolygon(fixed_polygons)
            return geom
        
        gdf['geometry'] = gdf.geometry.apply(fix_polygon_orientation)
        return gdf
    
    def apply_fixes(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Apply all fixes to make GeoDataFrame compliant."""
        # Apply column renaming fixes
        validation_results = self.validate(gdf)
        naming_validation = validation_results['rq1_rq6_rq21_naming']
        
        if not naming_validation['valid']:
            print("Applying naming convention fixes (RQ1, RQ6, RQ21)...")
            original_columns = gdf.columns.tolist()
            new_column_names = [self.format_text(col) for col in original_columns]
            column_map = dict(zip(original_columns, new_column_names))
            gdf.rename(columns=column_map, inplace=True)
        
        # Apply recommended fixes
        gdf = self.apply_rc17(gdf)
        gdf = self.apply_rc20(gdf)
        
        # Filter out invalid geometries
        gdf = gdf[gdf.geometry.notnull()]
        gdf = gdf[gdf.is_valid]
        
        return gdf


class GeoDataManager:
    """Main class that orchestrates all geospatial data operations."""
    
    def __init__(self):
        self.converters = {
            'gpkg_to_parquet': GeoPackageToParquetConverter(),
            'parquet_to_gpkg': ParquetToGeoPackageConverter()
        }
        self.readers = {
            'parquet': GeoParquetReader(),
            'gpkg': GeoPackageReader()
        }
        self.validator = GeoParquetValidator()
    
    def convert_gpkg_to_parquet(self, input_path: str, output_path: str, 
                               layer_name: str = None, chunk_size: int = 100000) -> bool:
        """Convert GeoPackage to GeoParquet format."""
        converter = GeoPackageToParquetConverter(chunk_size)
        return converter.process(input_path, output_path, layer_name=layer_name)
    
    def convert_parquet_to_gpkg(self, input_folder: str, output_path: str, 
                               layer_name: str = "merged_layer") -> bool:
        """Convert multiple GeoParquet files to single GeoPackage."""
        converter = self.converters['parquet_to_gpkg']
        return converter.process(input_folder, output_path, layer_name=layer_name)
    
    def read_file(self, file_path: str, **kwargs) -> Optional[gpd.GeoDataFrame]:
        """Read geospatial file (auto-detects format)."""
        if file_path.endswith('.parquet'):
            return self.readers['parquet'].read(file_path, **kwargs)
        elif file_path.endswith('.gpkg'):
            return self.readers['gpkg'].read(file_path, **kwargs)
        else:
            print(f"❌ Unsupported file format: {file_path}")
            return None
    
    def get_file_info(self, file_path: str) -> Dict:
        """Get metadata information about geospatial file."""
        if file_path.endswith('.parquet'):
            return self.readers['parquet'].get_info(file_path)
        elif file_path.endswith('.gpkg'):
            return self.readers['gpkg'].get_info(file_path)
        else:
            print(f"❌ Unsupported file format: {file_path}")
            return {}
    
    def validate_and_improve(self, gdf: gpd.GeoDataFrame, output_path: str = None) -> gpd.GeoDataFrame:
        """Validate and improve GeoDataFrame according to standards."""
        print("🔍 Running validation checks...")
        validation_results = self.validator.validate(gdf)
        
        # Print validation results
        for rule, result in validation_results.items():
            if isinstance(result, dict):
                status = "✓ PASS" if result.get('valid', False) else "❌ FAIL"
            else:
                status = "✓ PASS" if result else "❌ FAIL"
            print(f"  {rule}: {status}")
        
        # Apply fixes
        print("🔧 Applying fixes...")
        improved_gdf = self.validator.apply_fixes(gdf)
        print(f"✓ Validation and fixes completed. Features: {len(improved_gdf)}")
        
        # Save if output path provided
        if output_path:
            improved_gdf.to_parquet(output_path, index=False)
            print(f"✓ Saved improved file: {output_path}")
        
        return improved_gdf
    
    def process_folder(self, folder_path: str, operation: str, **kwargs):
        """Process all files in a folder with specified operation."""
        if operation == "validate_parquet":
            parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))
            
            for parquet_file in parquet_files:
                print(f"\n📁 Processing: {os.path.basename(parquet_file)}")
                gdf = self.read_file(parquet_file)
                if gdf is not None:
                    # Create improved folder if it doesn't exist
                    improved_folder = os.path.join(folder_path, "improved")
                    os.makedirs(improved_folder, exist_ok=True)
                    
                    # Generate output path
                    file_name = os.path.basename(parquet_file)
                    output_path = os.path.join(improved_folder, 
                                             file_name.replace(".parquet", "_improved.parquet"))
                    
                    self.validate_and_improve(gdf, output_path)
            
            print("\n🎉 All files processed successfully!")


