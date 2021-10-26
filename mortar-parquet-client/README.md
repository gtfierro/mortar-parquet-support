# Accessing the Static Mortar Dataset

All graphs are local to this repository; you will need to load them into a
database in order to query them. This can be done with the `setup-graphs.py` script.

```bash
# this will place the graphs into a database called 'models.db'
python setup-graphs.py graphs models.db
```

Now, you can import the client from `client.py` to fetch data:

```python
from client import Client

# change 'models.db' to match your local database as above; leave rest as-is
c = Client("models.db", "mortar-data/data", region="us-east-2")

# define a Brick query to fetch data; remember to use the
#  "brick:timeseries [ brick:hasTimeseriesId ?sen ] " construction to get the
# timeseries identifier
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

# get a DataFrame of the SPARQL query results
df = c.sparql(query1, sites=["bldg1", "bldg2"])
print(df.head())

# get a DataFrame of the timeseries data. WARNING THIS CAN BE VERY LARGE
df = c.data_sparql(query1, sites=["bldg1", "bldg2"], start='2016-01-01', end='2016-02-01', limit=1e6)
print(df.head())
```
