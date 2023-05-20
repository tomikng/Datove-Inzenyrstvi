#!/usr/bin/env python3
import argparse
import os

import pandas as pd

from rdflib import Graph, URIRef, BNode, Literal, Namespace, RDF, XSD, RDFS, SKOS, OWL, QB, DCTERMS

NS = Namespace("https://example.com/")
NSR = Namespace("https://example.com/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SDMX_CONCEPT = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
SDMX_CODE = Namespace("http://purl.org/linked-data/sdmx/2009/code#")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Process data and convert it to RDF.")
    parser.add_argument("--input-file", "-i", required=True,
                        help="path to the input file")
    return parser.parse_args()


def main():
    args = parse_arguments()

    file_path = os.path.abspath(args.input_file)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The input file '{file_path}' does not exist.")

    try:
        df = load_xlsx_as_df(file_path)
        data = parse_data(df)
        # print(data)
        rdf = as_rdf(data)
        print_rdf_as_trig(rdf)
    except Exception as e:
        print(f"An error occurred: {e}")


def parse_data(df: pd.DataFrame):
    df_r = get_region(df)
    codes_r = []
    regions = []
    populations = []
    mean_ages = []
    districts = []
    codes_k = []

    district = ''

    for i, row in df_r.iterrows():
        if pd.notna(row[0]) and not pd.notna(
                row[1]):  # if the first column is not empty but others are, it is name of the district
            district = row[0]
            continue
        else:
            codes_r.append(row[0])
            regions.append(row[1])
            populations.append(row[2])
            mean_ages.append(row[5])
            districts.append(district)
            codes_k.append(get_kraj_codes(df, district))

    # Get data from Praha
    df_praha = get_praha(df)
    codes_r.append(df_praha[0])
    regions.append(df_praha[1])
    districts.append(df_praha[1])
    populations.append(df_praha[2])
    mean_ages.append(df_praha[5])
    codes_k.append(df_praha[0])

    # create a new DataFrame from the lists
    new_df = pd.DataFrame({
        'Region_Code': codes_r,
        'Region': regions,
        'Population': populations,
        'MeanAge': mean_ages,
        'District': districts,
        'Kraj_code': codes_k
    })

    return new_df


def load_xlsx_as_df(file_path: str):
    df = pd.read_excel(file_path, sheet_name='List1', verbose=False, header=None)

    return df


def get_praha(df: pd.DataFrame):
    return df.iloc[17, :]


def get_region(df: pd.DataFrame):
    return df.iloc[32:]


def get_kraj_codes(df: pd.DataFrame, kraj: str):
    # df = df.iloc[17, :]
    for i, row in df.iterrows():
        if row[1] == kraj:
            return row[0]

    return None


def print_rdf_as_trig(graph: Graph):
    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/population.ttl", "w", encoding="utf-8") as f:
        f.write(graph.serialize(format="turtle"))
        print("Success, the file is in out/population.ttl")


def as_rdf(content):
    result = Graph(bind_namespaces="rdflib")

    concept_schema(result)
    resource_classes(result)
    resources(result, content)
    dimension = dimensions(result)
    measures = measure(result)
    structures = structure(result, dimension, measures)
    datasets = dataset(result, structures)
    observations(result, datasets, content)

    result = create_hierarchy(result)

    return result


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

            if place_type == "county":
                region_uri = NSR[f"region/{place['Kraj_code']}"]
                graph.add((uri, SKOS.broader, region_uri))
                graph.add((region_uri, SKOS.narrower, uri))

    add_place("county", "Region_Code", "Region", NSR.county)
    add_place("region", "Kraj_code", "District", NSR.region)

def dimensions(graph: Graph):
    dimensions_ = []

    for dim in [
        ("county", "Okres", NSR.County, SDMX_CONCEPT.refArea, NSR.county),
        ("region", "Kraj", NSR.Region, SDMX_CONCEPT.refArea, NSR.region),
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
    measure_prop = NS.meanPopulation
    graph.add((measure_prop, RDF.type, RDFS.Property))
    graph.add((measure_prop, RDF.type, QB.MeasureProperty))
    graph.add((measure_prop, RDFS.label, Literal("Průměrný věk obyvatel", lang="cs")))
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
    dataset = NSR.MeanPopulation2021
    graph.add((dataset, RDF.type, QB.DataSet))
    graph.add((dataset, RDFS.label, Literal("Průměrný věk obyvatel", lang="cs")))
    graph.add((dataset, DCTERMS.issued, Literal("2023-03-11", datatype=XSD.date)))
    graph.add((dataset, DCTERMS.modified, Literal("2023-03-29", datatype=XSD.date)))
    graph.add((dataset, DCTERMS.title, Literal("Průměrný věk obyvatel", lang="cs")))
    graph.add((dataset, DCTERMS.publisher, Literal("Hai Hung Nguyen")))
    graph.add((dataset, DCTERMS.license,
               Literal("https://github.com/tomikng/Datove-Inzenyrstvi/blob/main/hw1/LICENSE", datatype=XSD.anyURI)))
    graph.add((dataset, QB.structure, structure))

    return dataset


def observations(graph: Graph, dataset, data: pd.DataFrame):
    for index, row in data.iterrows():
        resource = NSR[f"observation-{index:04d}"]
        graph.add((resource, RDF.type, QB.Observation))
        graph.add((resource, QB.dataSet, dataset))
        graph.add((resource, NS.county, NSR[f"county/{row['Region_Code']}"]))
        graph.add((resource, NS.region, NSR[f"region/{row['Kraj_code']}"]))
        graph.add((resource, NS.meanPopulation, Literal(row["MeanAge"], datatype=XSD.integer)))


def create_hierarchy(graph: Graph) -> Graph:
    region = NS.region
    county = NS.county

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


if __name__ == "__main__":
    main()
