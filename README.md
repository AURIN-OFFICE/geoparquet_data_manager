# GeoParquet Manager

A comprehensive Python library for processing, converting, and validating geospatial data between GeoPackage and GeoParquet formats. Built with memory efficiency and data quality standards in mind.

## Features

🗺️ **Format Conversion**
- GeoPackage (.gpkg) to GeoParquet (.parquet) with chunked processing
- Multiple GeoParquet files to single GeoPackage

📊 **Data Validation & Quality**
- Some validation against geospatial data standards
- Automatic fixes for common data quality issues

⚡ **Memory Efficient**
- Chunked processing for large datasets
- Configurable chunk sizes
- Memory cleanup between operations

🔧 **Flexible Architecture**
- Modular design
- Individual components can be used independently
- Extensible for custom processors and validators

## Installation

### Requirements

Ensure you have Python 3.8+ installed, then install the required dependencies:

```bash
pip install -r requirements.txt
```

### Dependencies

```
geopandas>=0.14.0
pandas>=2.0.0
matplotlib>=3.7.0
numpy>=1.24.0
shapely>=2.0.0
pyarrow>=12.0.0
fiona>=1.9.0
pyproj>=3.5.0
```

## Quick Start

### Basic Usage

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
```

### Advanced Usage

#### Individual Components

```python
from geoparquet_manager import (
    GeoPackageToParquetConverter, 
    GeoParquetValidator,
    GeoParquetReader
)

# Custom chunked conversion
converter = GeoPackageToParquetConverter(chunk_size=50000)
success = converter.process("large_file.gpkg", "output.parquet")

# Direct validation
validator = GeoParquetValidator()
validation_results = validator.validate(gdf)
fixed_gdf = validator.apply_fixes(gdf)

# Direct reading with custom parameters
reader = GeoParquetReader()
gdf = reader.read("data.parquet")
metadata = reader.get_info("data.parquet")
```

#### Batch Processing

```python
# Process entire folders
manager.process_folder(
    folder_path="/path/to/parquet/files/",
    operation="validate_parquet"
)
```

## API Documentation

### GeoDataManager

Main orchestration class providing high-level interface for all operations.

#### Methods

- `convert_gpkg_to_parquet(input_path, output_path, layer_name=None, chunk_size=100000)` → bool
- `convert_parquet_to_gpkg(input_folder, output_path, layer_name="merged_layer")` → bool
- `read_file(file_path, **kwargs)` → Optional[gpd.GeoDataFrame]
- `get_file_info(file_path)` → Dict
- `validate_and_improve(gdf, output_path=None)` → gpd.GeoDataFrame
- `process_folder(folder_path, operation, **kwargs)`

### Converter Classes

#### GeoPackageToParquetConverter

Converts GeoPackage files to GeoParquet format using memory-efficient chunking.

```python
converter = GeoPackageToParquetConverter(chunk_size=100000)
success = converter.process(input_path, output_path, layer_name=None)
```

#### ParquetToGeoPackageConverter

Merges multiple GeoParquet files into a single GeoPackage.

```python
converter = ParquetToGeoPackageConverter()
success = converter.process(input_folder, output_path, layer_name="merged_layer")
```

### Reader Classes

#### GeoParquetReader

```python
reader = GeoParquetReader()
gdf = reader.read(file_path)
info = reader.get_info(file_path)
```

#### GeoPackageReader

```python
reader = GeoPackageReader()
gdf = reader.read(file_path, layer_name=None)
info = reader.get_info(file_path)
```

### Validator Class

#### GeoParquetValidator

Validates and fixes geospatial data according to established standards.

```python
validator = GeoParquetValidator()
results = validator.validate(gdf)
fixed_gdf = validator.apply_fixes(gdf)
```

## Validation Rules

The library implements comprehensive validation based on geospatial data standards:

### Required Rules (RQ)

- **RQ1/RQ6**: Column names must start with letter, use only lowercase a-z, numbers, underscores
- **RQ2**: Layers must have at least one feature
- **RQ15**: All geometries in a layer must be the same type
- **RQ21**: Names must not exceed 57 characters
- **RQ22**: Only approved EPSG spatial reference systems
- **RQ23**: Geometries must be valid and simple

### Recommended Rules (RC)

- **RC17**: Recommends naming geometry columns 'geom'
- **RC20**: Ensures proper polygon orientation (counter-clockwise exterior, clockwise holes)

### Supported EPSG Codes

The validator supports a comprehensive list of EPSG codes including:

- **Australian MGA zones (GDA94)**: 28348-28358
- **Australian MGA zones (GDA2020)**: 7846-7859
- **World/Web Mercator**: 3395, 3857
- **International standards**: 4326, 4258, 4936, 4937, and many others

## Memory Management

The library is designed for efficient processing of large geospatial datasets:

### Chunked Processing

```python
# Configure chunk size based on available memory
converter = GeoPackageToParquetConverter(chunk_size=50000)  # Smaller chunks for limited memory
converter = GeoPackageToParquetConverter(chunk_size=200000) # Larger chunks for more memory
```

### Memory Guidelines

- **Default chunk size**: 100,000 features
- **Low memory systems**: Use 10,000-50,000 features per chunk
- **High memory systems**: Use 200,000+ features per chunk
- **Very large files**: Monitor memory usage and adjust accordingly

## Error Handling

The library provides comprehensive error handling with informative messages:

```python
# All methods return boolean success indicators
success = manager.convert_gpkg_to_parquet("input.gpkg", "output.parquet")
if not success:
    print("Conversion failed - check error messages above")

# Validation provides detailed results
validation_results = validator.validate(gdf)
for rule, result in validation_results.items():
    if not result:
        print(f"Validation failed for rule: {rule}")
```

## File Structure

```
geoparquet_manager.py    # Main library file
requirements.txt         # Dependencies
README.md               # This documentation
```

## Examples

### Example 1: Convert Large GeoPackage

```python
from geoparquet_manager import GeoDataManager

manager = GeoDataManager()

# Convert with custom chunk size for memory efficiency
success = manager.convert_gpkg_to_parquet(
    input_path="large_dataset.gpkg",
    output_path="large_dataset.parquet",
    chunk_size=50000  # Smaller chunks for large files
)

if success:
    print("✓ Conversion completed successfully")
```

### Example 2: Validate and Improve Data Quality

```python
import geopandas as gpd
from geoparquet_manager import GeoDataManager

# Load data
manager = GeoDataManager()
gdf = gpd.read_file("raw_data.gpkg")

# Validate and fix issues
improved_gdf = manager.validate_and_improve(
    gdf=gdf,
    output_path="cleaned_data.parquet"
)

print(f"Original features: {len(gdf)}")
print(f"Cleaned features: {len(improved_gdf)}")
```

### Example 3: Batch Processing

```python
from geoparquet_manager import GeoDataManager

manager = GeoDataManager()

# Process all parquet files in a folder
manager.process_folder(
    folder_path="/data/parquet_files/",
    operation="validate_parquet"
)

# Results will be saved to /data/parquet_files/improved/
```

## Contributing

This library follows a modular architecture making it easy to extend:

1. **Add new processors**: Inherit from `GeoDataProcessor`
2. **Add new validators**: Inherit from `GeoDataValidator` 
3. **Add new readers**: Inherit from `GeoDataReader`


## Support

For issues, feature requests, or questions about usage, please refer to the inline documentation and examples provided in this README.
