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

    concept_schema(result)
    resource_classes(result)
    resources(result, content)
    result = create_hierarchy(result)
    dimension = dimensions(result)
    measures = measure(result)
    structures = structure(result, dimension, measures)
    datasets = dataset(result, structures)
    observations(result, datasets, content)
    return result


def print_rdf_as_trig(graph: Graph):
    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/care_providers.ttl", "w", encoding="utf-8") as f:
        f.write(graph.serialize(format="turtle"))
        print("Success, the file is in out/care_providers.ttl")


def concept_schema(graph: Graph):
    concepts = {
        NSR.region: "Kraj",
        NSR.county: "Okres",
        NSR.fieldOfCare: "Obor péče"
    }
    for uri, label in concepts.items():
        graph.add((uri, RDF.type, SKOS.ConceptScheme))
        graph.add((uri, RDFS.label, Literal(label, lang="cs")))
        graph.add((uri, SKOS.prefLabel, Literal(label, lang="cs")))


def resource_classes(graph: Graph):
    resource_dict = {
        NSR.County: "Okres",
        NSR.Region: "Kraj",
        NSR.FieldOfCare: "Obor péče"
    }

    for resource, label in resource_dict.items():
        graph.add((resource, RDF.type, RDFS.Class))
        graph.add((resource, RDF.type, OWL.Class))
        graph.add((resource, RDFS.label, Literal(label, lang="cs")))
        graph.add((resource, SKOS.prefLabel, Literal(label, lang="cs")))


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

    fields_of_care = data["OborPece"].unique()
    foc_index = pd.Index(fields_of_care)
    data["OborPeceCode"] = data["OborPece"].apply(lambda x: foc_index.get_loc(x))
    for index, field_of_care in enumerate(fields_of_care):
        uri = NSR[f"fieldOfCare/{index}"]
        graph.add((uri, RDF.type, SKOS.Concept))
        graph.add((uri, RDF.type, NSR.FieldOfCare))
        graph.add((uri, RDFS.label, Literal(field_of_care, lang="cs")))
        graph.add((uri, SKOS.prefLabel, Literal(field_of_care, lang="cs")))
        graph.add((uri, SKOS.inScheme, NSR.fieldOfCare))


def dimensions(graph: Graph):
    dimensions_ = []

    for dim in [
        ("county", "Okres", NSR.County, SDMX_CONCEPT.refArea, NSR.county),
        ("region", "Kraj", NSR.Region, SDMX_CONCEPT.refArea, NSR.region),
        ("fieldOfCare", "Obor péče", NSR.FieldOfCare, None, NSR.fieldOfCare),
    ]:
        prop_uri = NS[dim[0]]
        graph.add((prop_uri, RDF.type, RDFS.Property))
        graph.add((prop_uri, RDF.type, QB.DimensionProperty))
        graph.add((prop_uri, RDF.type, QB.CodedProperty))
        graph.add((prop_uri, RDFS.label, Literal(dim[1], lang="cs")))
        graph.add((prop_uri, RDFS.range, dim[2]))
        graph.add((prop_uri, QB.codeList, dim[4]))
        if dim[3]:
            graph.add((prop_uri, QB.concept, dim[3]))

        dimensions_.append(prop_uri)

    return dimensions_


def measure(graph: Graph):
    measure_prop = NS.numberOfCareProviders
    graph.add((measure_prop, RDF.type, RDFS.Property))
    graph.add((measure_prop, RDF.type, QB.MeasureProperty))
    graph.add((measure_prop, RDFS.label, Literal("Počet poskytovatelů péče", lang="cs")))
    graph.add((measure_prop, RDFS.range, XSD.integer))
    graph.add((measure_prop, RDFS.subPropertyOf, SDMX_MEASURE.obsValue))

    return [measure_prop]


def structure(graph: Graph, dimensions, measures):
    dsd = NS.dataStructureDefinition
    graph.add((dsd, RDF.type, QB.DataStructureDefinition))

    for dimension in dimensions:
        component = BNode()
        graph.add((dsd, QB.component, component))
        graph.add((component, QB.dimension, dimension))

    for measure in measures:
        component = BNode()
        graph.add((dsd, QB.component, component))
        graph.add((component, QB.measure, measure))

    return dsd


def dataset(graph: Graph, structure):
    dataset = NSR.CareProviders
    graph.add((dataset, RDF.type, QB.DataSet))
    graph.add((dataset, RDFS.label, Literal("Poskytovatelé zdravotních služeb", lang="cs")))
    graph.add((dataset, RDFS.label, Literal("Care Providers", lang="en")))
    graph.add((dataset, DCTERMS.issued, Literal("2023-03-11", datatype=XSD.date)))
    graph.add((dataset, DCTERMS.modified, Literal("2023-03-29", datatype=XSD.date)))
    graph.add((dataset, DCTERMS.title, Literal("Poskytovatelé zdravotních služeb", lang="cs")))
    graph.add((dataset, DCTERMS.title, Literal("Care Providers", lang="en")))
    graph.add((dataset, DCTERMS.publisher, Literal("Hai Hung Nguyen")))
    graph.add((dataset, DCTERMS.license,
               Literal("https://github.com/tomikng/Datove-Inzenyrstvi/blob/main/hw1/LICENSE", datatype=XSD.anyURI)))
    graph.add((dataset, QB.structure, structure))

    return dataset


def observations(graph: Graph, dataset, data: pd.DataFrame):
    grouped = data.groupby(["OkresCode", "KrajCode", "OborPeceCode"]).size().reset_index(name="PocetPoskytovaluPece")
    for index, row in grouped.iterrows():
        resource = NSR[f"observation-{index:04d}"]
        graph.add((resource, RDF.type, QB.Observation))
        graph.add((resource, QB.dataSet, dataset))
        graph.add((resource, NS.county, NSR[f"county/{row['OkresCode']}"]))
        graph.add((resource, NS.region, NSR[f"region/{row['KrajCode']}"]))
        graph.add((resource, NS.fieldOfCare, NSR[f"fieldOfCare/{row['OborPeceCode']}"]))
        graph.add((resource, NS.numberOfCareProviders, Literal(row["PocetPoskytovaluPece"], datatype=XSD.integer)))


def parse_arguments():
    parser = argparse.ArgumentParser(description="Process data and convert it to RDF.")
    parser.add_argument("--input-file", "-i", required=True,
                        help="path to the input file")
    return parser.parse_args()


def create_hierarchy(graph: Graph) -> Graph:
    region = NSR.Region
    county = NSR.County

    eurovoc_regauth = URIRef("http://eurovoc.europa.eu/6034")

    graph.add((eurovoc_regauth, RDF.type, SKOS.ConceptScheme))
    graph.add((eurovoc_regauth, SKOS.prefLabel, Literal("správní celek", lang="cs")))
    graph.add((eurovoc_regauth, SKOS.prefLabel, Literal("regional and local authorities", lang="en")))
    graph.add((eurovoc_regauth, SKOS.notation, Literal("6034")))

    graph.add((region, RDF.type, SKOS.ConceptScheme))
    graph.add((region, SKOS.inScheme, eurovoc_regauth))
    graph.add((region, SKOS.prefLabel, Literal("Kraj", lang="cs")))
    graph.add((region, SKOS.prefLabel, Literal("Region", lang="en")))

    graph.add((county, RDF.type, SKOS.ConceptScheme))
    graph.add((county, SKOS.inScheme, eurovoc_regauth))
    graph.add((county, SKOS.prefLabel, Literal("Okres", lang="cs")))
    graph.add((county, SKOS.prefLabel, Literal("County", lang="en")))

    return graph


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
