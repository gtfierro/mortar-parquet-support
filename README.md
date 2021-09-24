# Mortar and Parquet

We need a more efficient way to store timeseries data for Mortar. Many (potential) users of Mortar have asked for bulk downloads, and it would be good to support a storage solution that:

1. is much easier to manage than a server
2. supports efficient, bulk downloads

The proposed solution is storing partitioned Apache Parquet files in an S3-compatible storage server. Apache Parquet is a columnar data format which can efficiently store tabular data and can be partitioned on arbitrary keys. It is more efficient than CSV, but can easily be converted to CSV. It also is supported by a wide variety of programming languages, including [Python](https://arrow.apache.org/docs/python/parquet.html).

Amazon S3 provides dirt cheap storage and fast retrieval, which will simplify management of the data. [SeaweedFS](https://github.com/chrislusf/seaweedfs) is a widely-used, stable and open-source alternative.

## Mortar Data Model

The Mortar dataset will be organized into a table with the following schema:

| Column Name | Description | Example |
+-------------+-------------+---------+
| **Collection** | The name of the building or Brick model                                                | `bldg1`                                |
| **UUID**       | The unique 36-byte UUID for this data stream (sensor, setpoint, etc)                   | `42c44c24-1cf1-11ec-8cd1-1002b58053c7` |
| **Time**       | The RFC3339-encoded timestamp for a data reading; supports up to microsecond precision | `2020-01-01T12:34:56Z`                 |
| **Value**      | The floating-point value of this reading                                               | `3.1415`                               |
| **URI**        | The URI of this entity in the Brick model (*optional*)                                 | `urn:bldg1/sensor1`                    |
| **Label**      | The human-readable name of this data stream (*optional*)                               | `My Test Sensor`                    |

The table will be *partitioned* into two levels: first by **collection** then by **uuid***. This means that inside the Parquet directory, each collection will be its own folder. Inside each collection will be a folder for each UUID, i.e. for each data stream. Inside each UUID folder is a set of `.parquet` files. These are partitioned by size but the partitions are ordered by time.

For example:

```
top-level-data-directory/
├── _metadata
├── collection=bldg1
│   ├── uuid=01f5cba4-111b-42a4-bbb4-4844ec3ad6f6
│   │   └── 66faa7f1218d4fbeb959a17f007698c6.parquet
│   ├── uuid=0481a1cf-1092-4bd2-92ed-376f5c24bee5
│   │   └── 07a0335b078d4702b964158187fc73cf.parquet
│   ├── uuid=05d7494a-4653-4c30-8f66-9eaf964ee1a5
│   │   └── bd149291e7d046c08b20e4902b6477b0.parquet
├── collection=bldg2
│   ├── uuid=114049b6-6dfb-4035-90c7-daeb01e94506
│   │   └── c0d8efcacdeb4bbb83d358bc8d5fdf08.parquet
│   ├── uuid=2aa3a08a-82c0-4888-9503-1640f2c9bc0b
│   │   └── 907d63a370b0484aa903be34f240e1e6.parquet
│   ├── uuid=2d2f9eba-bee2-4cba-a3b0-9261656e0a1c
│   │   └── cb598a2f4b82409e93e948308b82d921.parquet
├── collection=bldg3
│   ├── uuid=0015fbe6-31dd-420c-b02b-636459bef652
│   │   └── bbf40ec63bdf4db08bc6bb34f98961c4.parquet
│   ├── uuid=0026aa30-fc3a-4440-b925-1fa3d63a2ef4
│   │   └── e87ceba034d848f49e394c90812dfce2.parquet
...
```

The `.parquet` files above will each contain compressed data for a single UUID

A `_metadata` file in the main Parquet directory contains statistics and other metadata about the entire dataset. This assists data processing by allowing clients to more efficiently find the data they are looking for without having to scan the whole dataset. Each `.parquet` file additionally contains the min/max/count statistics for each column. This is especially helpful for finding data that spans a certain time range.

## Writing Parquet Files

We will need to convert existing CSV dumps to Parquet directories; these directories can also be compressed (often quite effectively) to facilitate transmission.

`transform.py` is a script that will pull a CSV file into a Parquet directory.
It takes the following arguments:
- `collection`: the name of the building or dataset under which you want this data placed
- `csv_file`: the path to the CSV file. The CSV file must contain data for *one* stream
- `destination`: the path to the Parquet directory
- `time_column` (optional): the name of the column containing the timestamps. Defaults to `datetime`

The process of creating the `.parquet` files is pretty easy due to the great support from the PyArrow library.
If the `transform.py` script is not meeting your needs (for example, if the data CSVs are not named using UUIDs) then it should not be too onerous to adapt the script for your own needs. As long as the resulting file structure is the same and you are doing all of the necessary transformations (sort each CSV file by timestamp, ascending; make sure column names are right) you should be ok.

It is important to update the `_metadata` file after you are done pulling files into the Parquet directory. This can really be done at intermediate stages too, but as long as you run it once on the full dataset, everything will work out. This process is in the `make-metadata.py` script: `python make-metadata.py <parquet directory>`.

### Example

Assuming a data file `badd2ed0-1cf4-11ec-8cd1-1002b58053c7.csv` looks like the following:

```
datetime,1-3-10_MaxFlowHeatSP
2016-09-01T07:00:00Z,1235.0
2016-09-01T07:01:00Z,1235.0
2016-09-01T07:02:00Z,1235.0
2016-09-01T07:03:00Z,1235.0
2016-09-01T07:04:00Z,1235.0
```

The script will be executed as such:

```
python transform.py my-building badd2ed0-1cf4-11ec-8cd1-1002b58053c7.csv my-parquet-directory
```

**Note that the filename is a UUID**

If you have a directory of CSV files, you can write a short bash script to process all of them:

```bash
for file in `ls my-building-data-archive/*.csv`; do
    python transform.py my-building $file my-parquet-directory
done
```

When you are done with all the buildings, create a zip or tar.gz file archive of the Parquet directory.
