import dlt
from pathlib import Path
import pyarrow.parquet as pq
from typing import Iterator, Dict, Any


@dlt.resource(standalone=True)
def fs_resource(file_path: str) -> Iterator[Dict[str, str]]:
    """
    Lists and yields a single parquet file from the local filesystem
    """
    # Convert string path to Path object
    path = Path(file_path)
    
    # Verify file exists and is a parquet file
    if path.exists() and path.suffix == '.parquet':
        yield {
            "file_path": str(path.absolute()),
            "filename": path.name
        }
    else:
        raise FileNotFoundError(f"Parquet file not found at {file_path}")

@dlt.transformer
def parquet_reader(file_item: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """
    Loads a parquet file and yields its rows
    """
    # Read the parquet file using pyarrow
    table = pq.read_table(file_item['file_path'])
    
    # Convert to records and yield each one
    for record in table.to_pylist():
        yield record

# Create the pipeline
pipeline = dlt.pipeline(pipeline_name="case_study_pipeline", 
                        destination='postgres', 
                        dataset_name='case_study_raw')

# Create three extract pipes that list files from the file system and send them to the reader
file1_pipe = fs_resource("./data/raw/customers.parquet") | parquet_reader()
file2_pipe = fs_resource("./data/raw/orders.parquet") | parquet_reader()
file3_pipe = fs_resource("./data/raw/order_items.parquet") | parquet_reader()
file4_pipe = fs_resource("./data/raw/products.parquet") | parquet_reader()

# Run the pipeline with renamed resources to load to different tables
load_info = pipeline.run([
    file1_pipe.with_name("customers"),
    file2_pipe.with_name("orders"),
    file3_pipe.with_name("order_items"),
    file4_pipe.with_name("products"),
])
print(load_info)
