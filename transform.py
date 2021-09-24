import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import uuid
import os
from pathlib import Path

def transform(collection, csv_file_name, destination, time_col='datetime'):
    destination = Path(destination)
    _stream_id, _ = os.path.splitext(os.path.basename(csv_file_name))
    try:
        stream_id = uuid.UUID(_stream_id)
    except Exception as e:
        print(e)
        print(_stream_id)
        return

    df = pd.read_csv(csv_file_name)

    if time_col not in df.columns:
        print(df)
        print(f"could not find {time_col} in {csv_file_name}")
        return
    df['time'] = df.pop(time_col)
    df.set_index(pd.to_datetime(df.pop('time')), inplace=True)
    df.sort_index(ascending=True, inplace=True)

    stream_name = df.columns[0]
    print(stream_name)

    df['uuid'] = str(stream_id)
    df['value'] = df.pop(stream_name)
    df['label'] = stream_name
    df['uri'] = f"urn:{collection}/{stream_name}"
    df['collection'] = collection
    df = df[['uuid','value','collection','label','uri']]

    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, destination, partition_cols=['collection', 'uuid'], version='2.0', coerce_timestamps='us')

    return table


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 4:
        print("python transform.py collection csv_file destination time_col")
        sys.exit(1)
    table = transform(*sys.argv[1:])
    print(table)
