import os
import pandas as pd
import functools
import pyarrow as pa
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import fs
import rdflib
import glob

# TODO: provide method for id -> data
class Client:
    def __init__(self, db_dir, bucket, s3_endpoint=None, region=None):
        # monkey-patch RDFlib to deal with some issues w.r.t. oxrdflib
        def namespaces(self):
            if not self.store.namespaces():
                return []
            for prefix, namespace in self.store.namespaces():
                namespace = URIRef(namespace)
                yield prefix, namespace
        rdflib.namespace.NamespaceManager.namespaces = namespaces


        self.s3 = fs.S3FileSystem(endpoint_override=s3_endpoint, region=region)
        self.ds = ds.parquet_dataset(f'{bucket}/_metadata', partitioning='hive', filesystem=self.s3)
        self.store = rdflib.Dataset(store="OxSled")
        self.store.default_union = True # queries default to the union of all graphs
        self.store.open(db_dir)


    def _table_exists(self, table):
        try:
            res = self.data_cache.table(table)
            return res is not None
        except RuntimeError:
            return False

    def sparql(self, query, sites=None):
        if sites is None:
            res = self.store.query(query)
            rows = list(map(str, row) for row in res)
            df = pd.DataFrame.from_records(
                rows, columns=[str(c) for c in res.vars]
            )
            return df
        dfs = []
        for site in sites:
            graph_name = f"urn:{site}#"
            graph = self.store.graph(graph_name)
            res = graph.query(query)
            rows = list(map(str, row) for row in res)
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

    def _to_batches(self, sparql, sites=None, start=None, end=None, limit=None):
        res = self.sparql(sparql, sites=sites)
        start = pd.to_datetime("2000-01-01T00:00:00Z" if not start else start)
        end = pd.to_datetime("2100-01-01T00:00:00Z" if not end else end)
        uuids = list(set([str(item) for row in res.values for item in row]))
        f = (ds.field('uuid').isin(uuids)) & (ds.field("time") <= pa.scalar(end)) & (ds.field("time") >= pa.scalar(start))
        for batch in self.ds.to_batches(filter=f):
            yield batch

    def data_sparql_to_csv(self, sparql, filename, sites=None, start=None, end=None, limit=None):
        num = 0
        for batch in self._to_batches(sparql, sites=sites, start=start, end=end, limit=limit):
            df = batch.to_pandas()
            num += len(df)
            df.to_csv(filename, mode='a', header=False)
        return num

    def data_sparql_to_duckdb(self, sparql, database, table, sites=None, start=None, end=None, limit=None):
        import duckdb
        self.data_cache = duckdb.connect(database)
        for batch in self._to_batches(sparql, sites=sites, start=start, end=end, limit=limit):
            pq.write_table(pa.Table.from_batches([batch]), "tmp.parquet")
            if not self._table_exists(table):
                self.data_cache.execute(f"CREATE TABLE {table} AS SELECT * from parquet_scan('tmp.parquet')")
            else:
                self.data_cache.execute(f"INSERT INTO {table} SELECT * from parquet_scan('tmp.parquet')")
            os.remove("tmp.parquet")
        self.data_cache.commit()
        return self.data_cache.table(table)

    def data_sparql(self, sparql, sites=None, start=None, end=None, limit=None, in_memory=True):
        dfs = []
        for batch in self._to_batches(sparql, sites=sites, start=start, end=end, limit=limit):
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
    # currently offline.. hopefully back up soon
    #c = Client("graphs", "data", s3_endpoint="https://parquet.mortardata.org")
    c = Client("models.db", "mortar-data/data", region="us-east-2")

    all_points = """
        PREFIX brick: <https://brickschema.org/schema/Brick#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT * WHERE {
            ?point rdf:type/rdfs:subClassOf* brick:Point .
            ?point rdf:type ?type .
            ?point brick:timeseries [ brick:hasTimeseriesId ?id ] .
        }
    """
    df = c.sparql(all_points, sites=["bldg1", "bldg2"])
    df.to_csv("all_points.csv")

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
    df.to_csv("query1_sparql.csv")
    print(df.head())

    df = c.data_sparql(query1, sites=["bldg1", "bldg2"], start='2016-01-01', end='2016-02-01', limit=1e6)
    print(df.head())

    res = c.data_sparql_to_csv(query1, "query1.csv", sites=["bldg1"])
    print(res)

"""
Notes:
- if you download of data, it all goes in memory:
    - can we store data locally?
    - insert into local parquet file? or duckdb?
"""
