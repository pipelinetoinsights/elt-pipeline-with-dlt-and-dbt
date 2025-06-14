import pytest
from ingestion_pipeline import fs_resource, parquet_reader
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import os


# Helper to create a temporary parquet file
def create_temp_parquet(data: list[dict]) -> str:
    table = pa.Table.from_pylist(data)
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
    pq.write_table(table, tmp_file.name)
    return tmp_file.name


def test_fs_resource_returns_valid_dict():
    # Create a temp parquet file
    file_path = create_temp_parquet([{"id": 1, "name": "Alice"}])

    result = list(fs_resource(file_path))

    assert isinstance(result, list)
    assert result[0]["file_path"].endswith(".parquet")
    assert result[0]["filename"].endswith(".parquet")

    os.unlink(file_path)  # Clean up


def test_fs_resource_raises_file_not_found():
    with pytest.raises(FileNotFoundError):
        list(fs_resource("nonexistent_file.parquet"))


def test_fs_resource_raises_if_not_parquet():
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
    tmp_file.write(b"not parquet content")
    tmp_file.close()

    with pytest.raises(FileNotFoundError):
        list(fs_resource(tmp_file.name))

    os.unlink(tmp_file.name)


def test_parquet_reader_yields_rows_correctly():
    data = [{"id": 1, "amount": 100}, {"id": 2, "amount": 200}]
    file_path = create_temp_parquet(data)

    file_item = {"file_path": file_path}
    rows = list(parquet_reader(file_item))

    assert rows == data

    os.unlink(file_path)
