import sys
from pathlib import Path
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

def make_metadata(parquet_dir):
    root_path = Path(parquet_dir)
    metadata_collector = []
    d = pq.ParquetDataset(root_path, use_legacy_dataset=False)
    new_md = {}
    for piece in d.fragments:
        md = piece.metadata
        md.set_file_path(piece.path.split('/', 1)[-1])
        metadata_collector.append(md)
    pq.write_metadata(d.schema, root_path / "_common_metadata", version='2.0')
    pq.write_metadata(md.schema.to_arrow_schema(), root_path / "_metadata", metadata_collector=metadata_collector, version='2.0')

    # now try it out; should load successfully
    d = ds.parquet_dataset(root_path / "_metadata", partitioning='hive')
    print("Successfully created metadata file for", d)
    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python make-metadata.py <parquet-directory>")
        sys.exit(1)
    make_metadata(sys.argv[1])
