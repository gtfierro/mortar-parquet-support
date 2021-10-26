import os
import pandas as pd
import functools
import pyarrow as pa
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pyarrow import fs
import rdflib
import glob


class Client:
    def __init__(self, db_dir, bucket, s3_endpoint=None, region=None):
        self.s3 = fs.S3FileSystem(endpoint_override=s3_endpoint, region=region)
        self.ds = ds.parquet_dataset(f'{bucket}/_metadata', partitioning='hive', filesystem=self.s3)
        self.store = rdflib.Dataset(store="OxSled")
        self.store.default_union = True # queries default to the union of all graphs
        self.store.open(db_dir)

    def sparql(self, query, sites=None):
        if sites is None:
            res = self.store.query(query)
            rows = list(res)
            df = pd.DataFrame.from_records(
                rows, columns=[str(c) for c in res.vars]
            )
            return df
        dfs = []
        for site in sites:
            graph_name = f"urn:{site}#"
            graph = self.store.graph(graph_name)
            res = graph.query(query)
            rows = list(res)
            df = pd.DataFrame.from_records(
                rows, columns=[str(c) for c in res.vars]
            )
            df["site"] = site
            dfs.append(df)
        if len(dfs) == 0:
            return pd.DataFrame()
        if len(dfs) == 1:
            return dfs[0]
        return functools.reduce(lambda x, y: pd.concat([x, y], axis=0), dfs)
            
        
    def data_sparql(self, sparql, sites=None, start=None, end=None, limit=None):
        res = self.sparql(sparql, sites=sites)
        start = pd.to_datetime("2000-01-01T00:00:00Z" if not start else start)
        end = pd.to_datetime("2100-01-01T00:00:00Z" if not end else end)
        uuids = list(set([str(item) for row in res.values for item in row]))
        f = (ds.field('uuid').isin(uuids)) & (ds.field("time") <= pa.scalar(end)) & (ds.field("time") >= pa.scalar(start))
        dfs = []
        for batch in self.ds.to_batches(filter=f):
            df = batch.to_pandas()
            print(f"Downloaded batch of {len(df)} records")
            dfs.append(df)
            if limit:
                limit -= len(df)
                if limit <= 0:
                    break
        if len(dfs) == 0:
            return pd.DataFrame()
        if len(dfs) == 1:
            return dfs[0]
        return functools.reduce(lambda x, y: pd.concat([x, y], axis=0), dfs)

if __name__ == '__main__':
    #c = Client("graphs", "data", s3_endpoint="https://parquet.mortardata.org")
    c = Client("graph.db", "mortar-data/data", region="us-east-2")
    query1 = """
        PREFIX brick: <https://brickschema.org/schema/Brick#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?vav ?sen ?sp  WHERE {
        ?sen_point rdf:type/rdfs:subClassOf* brick:Temperature_Sensor ;
            brick:timeseries [ brick:hasTimeseriesId ?sen ] .
        ?sp_point rdf:type/rdfs:subClassOf* brick:Temperature_Setpoint ;
            brick:timeseries [ brick:hasTimeseriesId ?sp ] .
        ?vav a brick:VAV .
        ?vav brick:hasPoint ?sen_point, ?sp_point .
    }"""
    df = c.sparql(query1, sites=["bldg1", "bldg2"])
    print(df.head())


    df = c.data_sparql(query1, sites=["bldg1", "bldg2"], start='2016-01-01', end='2016-02-01', limit=1e6)
    print(df.head())

"""
Notes:
- if you download of data, it all goes in memory:
    - can we store data locally?
    - insert into local parquet file? or duckdb?
"""
