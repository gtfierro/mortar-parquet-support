for file in `ls example-data/*.csv` ; do
    python transform.py my-building $file my-parquet-directory
done
