import argparse
from pathlib import Path
import glob
import rdflib
import os

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("graph_dir", help="Folder containing .ttl files", type=str, default="graphs")
    parser.add_argument("db_dir", help="Destination of on-disk database", type=str, default="graph.db")
    args = parser.parse_args()

    store = rdflib.Dataset(store="OxSled")
    store.default_union = True # queries default to the union of all graphs
    store.open(args.db_dir)
    for ttlfile in glob.glob(str(Path(args.graph_dir) / "*.ttl")):
        graph_name = os.path.splitext(os.path.basename(ttlfile))[0]
        graph_name = f"urn:{graph_name}#"
        graph = store.graph(graph_name)
        print(f"Loading {ttlfile} => ", end='', flush=True)
        graph.parse(ttlfile, format="ttl")
        graph.parse("https://github.com/BrickSchema/Brick/releases/download/nightly/Brick.ttl", format="ttl")
        print(f"Done as {graph_name}")
