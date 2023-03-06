#!/usr/bin/env python3
import os

import pandas as pd

from rdflib import Graph, URIRef, BNode, Literal, Namespace, RDF, XSD, RDFS


def main():
    file_path = "populace-okresy-2021.xlsx"
    df = load_xlsx_as_df(file_path)
    data = parse_data(df)
    rdf = as_rdf(data)
    print_rdf_as_trig(rdf)


def parse_data(df: pd.DataFrame):
    df_r = get_region(df)

    codes = []
    regions = []
    populations = []
    mean_ages = []
    districts = []

    district = ''

    for i, row in df_r.iterrows():
        if pd.notna(row[0]) and not pd.notna(row[1]):  # if the first column is not empty but others are, it is name of the district
            district = row[0]
            continue
        else:
            codes.append(row[0])

            regions.append(row[1])
            populations.append(row[2])
            mean_ages.append(row[5])
            districts.append(district)

    # Get data from Praha
    df_praha = get_praha(df)
    codes.append(df_praha[0])
    regions.append(df_praha[1])
    districts.append(df_praha[1])
    populations.append(df_praha[2])
    mean_ages.append(df_praha[5])

    # create a new DataFrame from the lists
    new_df = pd.DataFrame({
        'Code': codes,
        'Region': regions,
        'Population': populations,
        'MeanAge': mean_ages,
        'District': districts,
    })

    # display the resulting DataFrame
    return new_df.to_dict(orient='records')


def load_xlsx_as_df(file_path: str):
    df = pd.read_excel(file_path, sheet_name='List1', verbose=False, header=None)

    return df


def get_praha(df: pd.DataFrame):
    return df.iloc[17, :]


def get_region(df: pd.DataFrame):
    return df.iloc[32:]


def as_rdf(content):
    result = Graph()
    ns_manager = result.namespace_manager

    # Define a new namespaces
    ex_ns = Namespace("http://example.com/")
    ns_manager.bind("ex", ex_ns)

    qb_ns = Namespace("http://purl.org/linked-data/cube#")
    ns_manager.bind("qb", qb_ns)

    skos_ns = Namespace("http://www.w3.org/2004/02/skos/core#")
    ns_manager.bind("skos", skos_ns)

    dct_ns = Namespace("http://purl.org/dc/terms/")
    ns_manager.bind("dct", dct_ns)

    xsd_ns = Namespace("http://www.w3.org/2001/XMLSchema#")
    ns_manager.bind("xsd", xsd_ns)

    sdmx_ns = Namespace("http://purl.org/linked-data/sdmx#")
    ns_manager.bind("sdmx", sdmx_ns)

    sdmx_concept_ns = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
    ns_manager.bind("sdmx_concept", sdmx_concept_ns)

    sdmx_dimension_ns = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
    ns_manager.bind("sdmx-dimension", sdmx_dimension_ns)

    sdmx_measure_ns = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")
    ns_manager.bind("sdmx-measure", sdmx_measure_ns)

    # Triplets
    # Dataset
    result.add((ex_ns['dataset-population'], RDF.type, qb_ns['DataSet']))
    result.add(
        (ex_ns['dataset-population'], skos_ns['prefLabel'], Literal("Průměrný věk obyvatelstva České republiky v roce 2021", lang='cs')))
    result.add((ex_ns['dataset-population'], qb_ns['structure'], ex_ns['structure']))
    result.add((ex_ns['dataset-population'], dct_ns['issued'], Literal("2021-01-01", datatype=XSD.date)))
    result.add((ex_ns['dataset-population'], dct_ns['modified'], Literal("2021-01-01", datatype=XSD.date)))
    result.add((ex_ns['dataset-population'], dct_ns['publisher'], URIRef("https://www.czso.cz/csu/czso/")))
    result.add((ex_ns['dataset-population'], dct_ns['license'],
                URIRef("https://data.gov.cz/podm%C3%ADnky-u%C5%BEit%C3%AD/voln%C3%BD-p%C5%99%C3%ADstup/")))

    # Data Structure Definitions
    structure = BNode()
    result.add((ex_ns['structure'], RDF.type, qb_ns['MeasureProperty']))
    result.add((ex_ns['structure'], qb_ns['component'], structure))
    result.add((structure, qb_ns['dimension'], ex_ns['Okres']))
    result.add((structure, qb_ns['dimension'], ex_ns['Kraj']))
    result.add((structure, qb_ns['measure'], ex_ns['PrumernyVek']))

    # sdmx
    # Concept
    result.add((sdmx_concept_ns.Okres, RDF.type, sdmx_ns.Concept))
    result.add((sdmx_concept_ns.Okres, RDF.type, skos_ns.Concept))
    result.add((sdmx_concept_ns.Okres, RDFS.label, Literal("Okres", lang='cs')))
    result.add((sdmx_concept_ns.Okres, skos_ns.prefLabel, Literal("Okres", lang='cs')))
    result.add((sdmx_concept_ns.Okres, RDFS.comment, Literal("Administrativní členění země nebo regionu.", lang='cs')))

    result.add((sdmx_concept_ns.Kraj, RDF.type, sdmx_ns.Concept))
    result.add((sdmx_concept_ns.Kraj, RDF.type, skos_ns.Concept))
    result.add((sdmx_concept_ns.Kraj, RDFS.label, Literal("Kraj", lang='cs')))
    result.add((sdmx_concept_ns.Kraj, skos_ns.prefLabel, Literal("Kraj", lang='cs')))
    result.add((sdmx_concept_ns.Kraj, RDFS.comment, Literal("Geografická oblast nebo skupina okresů.", lang='cs')))

    result.add((sdmx_concept_ns.Prumer, RDF.type, sdmx_ns.Concept))
    result.add((sdmx_concept_ns.Prumer, RDF.type, skos_ns.Concept))
    result.add((sdmx_concept_ns.Prumer, RDFS.label, Literal("Prumer", lang='cs')))
    result.add((sdmx_concept_ns.Prumer, skos_ns.prefLabel, Literal("Prumer", lang='cs')))
    result.add((sdmx_concept_ns.Prumer, RDFS.comment, Literal("Prumer nejakeho mnozstvi", lang='cs')))

    # sdmx-dimenze
    result.add((sdmx_dimension_ns.Okres, RDF.type, qb_ns.DimensionProperty))
    result.add((sdmx_dimension_ns.Okres, RDF.type, RDF.Property))
    result.add((sdmx_dimension_ns.Okres, RDFS.range, RDFS.Resource))
    result.add((sdmx_dimension_ns.Okres, qb_ns.concept, sdmx_concept_ns.Okres))
    result.add((sdmx_dimension_ns.Okres, RDFS.label, Literal("Okres", lang="cs")))
    result.add((sdmx_dimension_ns.Okres, skos_ns.prefLabel, Literal("Okres", lang="cs")))
    result.add((sdmx_dimension_ns.Okres, RDFS.comment, Literal("Konkrétní administrativní členění", lang="cs")))

    result.add((sdmx_dimension_ns.Kraj, RDF.type, qb_ns.DimensionProperty))
    result.add((sdmx_dimension_ns.Kraj, RDF.type, RDF.Property))
    result.add((sdmx_dimension_ns.Kraj, RDFS.range, RDFS.Resource))
    result.add((sdmx_dimension_ns.Kraj, qb_ns.concept, sdmx_concept_ns.Kraj))
    result.add((sdmx_dimension_ns.Kraj, RDFS.label, Literal("Kraj", lang="cs")))
    result.add((sdmx_dimension_ns.Kraj, skos_ns.prefLabel, Literal("Kraj", lang="cs")))
    result.add(
        (sdmx_dimension_ns.Kraj, RDFS.comment, Literal("Rozsáhlejší geografická oblast nebo skupina okresů", lang="cs")))

    # sdmx-measure
    result.add((sdmx_measure_ns.PrumernyVek, RDF.type, qb_ns.MeasureProperty))
    result.add((sdmx_measure_ns.PrumernyVek, RDF.type, RDF.Property))
    result.add((sdmx_measure_ns.PrumernyVek, qb_ns.Concept, sdmx_concept_ns.Prumer))
    result.add((sdmx_measure_ns.PrumernyVek, RDFS.label, Literal("PrumernyVek", lang="cs")))
    result.add((sdmx_measure_ns.PrumernyVek, skos_ns.prefLabel, Literal("PrumernyVek", lang="cs")))
    result.add((sdmx_measure_ns.PrumernyVek, RDFS.comment, Literal(
        "Prumerny vek osob v danem regionu",
        lang="cs")))

    # Dimensions
    result.add((ex_ns.Okres, RDF.type, RDF.Property))
    result.add((ex_ns.Okres, RDF.type, qb_ns.DimensionProperty))
    result.add((ex_ns.Okres, RDFS.label, Literal("Okres", lang="cs")))
    result.add((ex_ns.Okres, skos_ns.prefLabel, Literal("Okres", lang="cs")))
    result.add((ex_ns.Okres, RDFS.subPropertyOf, sdmx_dimension_ns.Okres))
    result.add((ex_ns.Okres, qb_ns.Concept, sdmx_concept_ns.Okres))
    result.add((ex_ns.Okres, RDFS.range, RDFS.Resource))

    result.add((ex_ns.Kraj, RDF.type, RDF.Property))
    result.add((ex_ns.Kraj, RDF.type, qb_ns.DimensionProperty))
    result.add((ex_ns.Kraj, RDFS.label, Literal("Kraj", lang="cs")))
    result.add((ex_ns.Kraj, skos_ns.prefLabel, Literal("Kraj", lang="cs")))
    result.add((ex_ns.Kraj, RDFS.subPropertyOf, sdmx_dimension_ns.Kraj))
    result.add((ex_ns.Kraj, qb_ns.Concept, sdmx_concept_ns.Kraj))
    result.add((ex_ns.Kraj, RDFS.range, RDFS.Resource))

    result.add((ex_ns.PrumernyVek, RDF.type, RDF.Property))
    result.add((ex_ns.PrumernyVek, RDF.type, qb_ns.DimensionProperty))
    result.add((ex_ns.PrumernyVek, RDFS.label, Literal("PrumernyVek", lang="cs")))
    result.add((ex_ns.PrumernyVek, skos_ns.prefLabel, Literal("PrumernyVek", lang="cs")))
    result.add((ex_ns.PrumernyVek, RDFS.subPropertyOf, sdmx_measure_ns.PrumernyVek))
    result.add((ex_ns.PrumernyVek, qb_ns.Concept, sdmx_concept_ns.Prumer))
    result.add((ex_ns.PrumernyVek, RDFS.range, RDFS.Resource))

    counter = 0
    for record in content:
        resource = URIRef(f"{ex_ns}observation-{counter:03}")
        result.add((resource, RDF.type, qb_ns.Observation))
        result.add((resource, qb_ns.dataSet, qb_ns["dataset-population"]))
        result.add((resource, ex_ns.Okres, Literal(record["Region"])))
        result.add((resource, ex_ns.Kraj, Literal(record["District"])))
        result.add((resource, ex_ns.PrumernyVek, Literal(record["MeanAge"])))
        counter = counter + 1

    return result


def print_rdf_as_trig(graph: Graph):
    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/population.ttl", "w", encoding="utf-8") as f:
        f.write(graph.serialize(format="turtle"))
        print("Success, the file is in out/population.ttl")


if __name__ == "__main__":
    main()
