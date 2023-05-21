#!/usr/bin/env python3
import argparse
import csv
import os
import pprint

import pandas as pd

from rdflib import Graph, URIRef, BNode, Literal, Namespace, RDF, XSD, SKOS, OWL, QB, DCTERMS

NS = Namespace("https://example.com/")
NSR = Namespace("https://example.com/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")


def load_csv_file_as_object(file_path: str):
    result = pd.read_csv(file_path, low_memory=False)
    return result


def as_rdf(content):
    result = Graph(bind_namespaces="rdflib")


    resources(result, content)

    return result


def print_rdf_as_trig(graph: Graph):
    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/hierarchy.ttl", "w", encoding="utf-8") as f:
        f.write(graph.serialize(format="turtle"))
        print("Success, the file is in out/hierarchy.ttl")




def resources(graph: Graph, data: pd.DataFrame):
    def uri_for(uri_str: str) -> URIRef:
        return URIRef(uri_str)

    def add_place(place_type: str, code_col: str, name_col: str, scheme: URIRef):
        places = data.drop_duplicates(code_col)[[code_col, name_col]].dropna(subset=[code_col])
        for _, place in places.iterrows():
            uri = NSR[f"{place_type}/{place[code_col]}"]
            graph.add((uri, RDF.type, SKOS.Concept))
            graph.add((uri, RDF.type, SDMX_CODE.Area))
            graph.add((uri, RDF.type, getattr(NSR, place_type)))
            graph.add((uri, RDFS.label, Literal(place[name_col], lang="cs")))
            graph.add((uri, SKOS.prefLabel, Literal(place[name_col], lang="cs")))
            graph.add((uri, SKOS.inScheme, uri_for(scheme)))
            graph.add((uri, SKOS.inScheme, SDMX_CODE.area))

    add_place("county", "OkresCode", "Okres", NSR.county)
    add_place("region", "KrajCode", "Kraj", NSR.region)

    # skos:broader / skos:narrower
    for _, row in data.iterrows():
        county_uri = NSR[f"county/{row['OkresCode']}"]
        region_uri = NSR[f"region/{row['KrajCode']}"]
        graph.add((county_uri, SKOS.broader, region_uri))
        graph.add((region_uri, SKOS.narrower, county_uri))


def parse_arguments():
    parser = argparse.ArgumentParser(description="Create a schema hierarchy.")
    parser.add_argument("--input-file", "-i", required=True,
                        help="path to the input file")
    return parser.parse_args()


def main():
    args = parse_arguments()

    file_path = os.path.abspath(args.input_file)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The input file '{file_path}' does not exist.")

    try:
        data = load_csv_file_as_object(file_path)
        rdf = as_rdf(data)
        print_rdf_as_trig(rdf)
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
