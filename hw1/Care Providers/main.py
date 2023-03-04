#!/usr/bin/env python3
import csv
import pprint

import pandas as pd

from rdflib import Graph, URIRef, BNode, Literal, Namespace, RDF, XSD, RDFS


def process_data(data):
    # Create a pandas dataframe from the data dictionary
    df = pd.DataFrame.from_dict(data)

    # Group by Okres and aggregate the data by count of ICO and first value of Kraj and Obor Pece columns
    df_grouped = df.groupby('Okres').agg({'Ico': 'count', 'Kraj': 'first', 'OborPece': 'first', "Okres": "first"})

    # Rename the columns for better readability
    df_grouped = df_grouped.rename(columns={'Ico': 'PocetPoskytovateluPece'})
    #
    # pd.set_option('display.max_rows', None)
    # pd.set_option('display.max_columns', None)
    #
    # print(df_grouped)
    return df_grouped.to_dict(orient='records')


def main():
    file_path = "Care Providers/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv"
    data = load_csv_file_as_object_and_aggregate(file_path)
    data = process_data(data)
    # pprint.pprint(data)
    # print(data)
    rdf = as_rdf(data)
    print_rdf_as_trig(rdf)


def load_csv_file_as_object_and_aggregate(file_path: str):
    # define the columns we want to extract
    columns = ['Okres', 'Kraj', 'OborPece', 'Ico']

    # initialize an empty list to store the extracted data
    data = []

    # read in the data from the file
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            # extract the desired columns from the row as a dictionary
            extracted_row = {col: row[col] for col in columns}
            # add the extracted row to the data list
            data.append(extracted_row)

    return data


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
    result.add((ex_ns['dataCubeInstance'], RDF.type, qb_ns['DataSet']))
    result.add(
        (ex_ns['dataCubeInstance'], skos_ns['prefLabel'], Literal("Pokytovatelé zdravotních služeb", lang='cs')))
    result.add((ex_ns['dataCubeInstance'], qb_ns['structure'], ex_ns['structure']))
    result.add((ex_ns['dataCubeInstance'], dct_ns['issued'], Literal("2023-03-01", datatype=XSD.date)))
    result.add((ex_ns['dataCubeInstance'], dct_ns['modified'], Literal("2023-03-01", datatype=XSD.date)))
    result.add((ex_ns['dataCubeInstance'], dct_ns['publisher'], URIRef("https://nrpzs.uzis.cz/")))
    result.add((ex_ns['dataCubeInstance'], dct_ns['license'], URIRef("https://creativecommons.org/licenses/by/4.0/")))

    # Data Structure Definitions
    structure = BNode()
    result.add((ex_ns['structure'], RDF.type, qb_ns['MeasureProperty']))
    result.add((ex_ns['structure'], qb_ns['component'], structure))
    result.add((structure, qb_ns['dimension'], ex_ns['Okres']))
    result.add((structure, qb_ns['dimension'], ex_ns['Kraj']))
    result.add((structure, qb_ns['dimension'], ex_ns['OborPece']))
    result.add((structure, qb_ns['measure'], ex_ns['PocetPoskytovatelu']))

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

    result.add((sdmx_concept_ns.OborPece, RDF.type, sdmx_ns.Concept))
    result.add((sdmx_concept_ns.OborPece, RDF.type, skos_ns.Concept))
    result.add((sdmx_concept_ns.OborPece, RDFS.label, Literal("OborPece", lang='cs')))
    result.add((sdmx_concept_ns.OborPece, skos_ns.prefLabel, Literal("OborPece", lang='cs')))
    result.add((sdmx_concept_ns.OborPece, RDFS.comment,
                Literal("Typ poskytované péče, jako například lékařská, dentální nebo duševní zdraví.", lang='cs')))

    result.add((sdmx_concept_ns.Pocet, RDF.type, sdmx_ns.Concept))
    result.add((sdmx_concept_ns.Pocet, RDF.type, skos_ns.Concept))
    result.add((sdmx_concept_ns.Pocet, RDFS.label, Literal("Pocet", lang='cs')))
    result.add((sdmx_concept_ns.Pocet, skos_ns.prefLabel, Literal("Pocet", lang='cs')))
    result.add((sdmx_concept_ns.Pocet, RDFS.comment, Literal("Pocet nejakeho mnozstvi", lang='cs')))

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
        (sdmx_dimension_ns.Kraj, RDFS.comment, Literal("ozsáhlejší geografická oblast nebo skupina okresů", lang="cs")))

    result.add((sdmx_dimension_ns.OborPece, RDF.type, qb_ns.DimensionProperty))
    result.add((sdmx_dimension_ns.OborPece, RDF.type, RDF.Property))
    result.add((sdmx_dimension_ns.OborPece, RDFS.range, RDFS.Resource))
    result.add((sdmx_dimension_ns.OborPece, qb_ns.concept, sdmx_concept_ns.OborPece))
    result.add((sdmx_dimension_ns.OborPece, RDFS.label, Literal("OborPece", lang="cs")))
    result.add((sdmx_dimension_ns.OborPece, skos_ns.prefLabel, Literal("OborPece", lang="cs")))
    result.add((sdmx_dimension_ns.OborPece, RDFS.comment,
                Literal("Typ poskytované péče, jako například lékařská, dentální nebo duševní zdraví.", lang="cs")))

    # sdmx-measure
    result.add((sdmx_measure_ns.PocetPoskytovatelu, RDF.type, qb_ns.MeasureProperty))
    result.add((sdmx_measure_ns.PocetPoskytovatelu, RDF.type, RDF.Property))
    result.add((sdmx_measure_ns.PocetPoskytovatelu, qb_ns.Concept, sdmx_concept_ns.Pocet))
    result.add((sdmx_measure_ns.PocetPoskytovatelu, RDFS.label, Literal("Pocet poskytovatelu", lang="cs")))
    result.add((sdmx_measure_ns.PocetPoskytovatelu, skos_ns.prefLabel, Literal("Pocet poskytovatelu", lang="cs")))
    result.add((sdmx_measure_ns.PocetPoskytovatelu, RDFS.comment, Literal(
        "Skutečný počet osob poskytující péči v určitém okrese, regionu a/nebo oblasti péče po určité časové období.",
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
    result.add((ex_ns.Kraj, RDFS.label, Literal("Okres", lang="cs")))
    result.add((ex_ns.Kraj, skos_ns.prefLabel, Literal("Okres", lang="cs")))
    result.add((ex_ns.Kraj, RDFS.subPropertyOf, sdmx_dimension_ns.Kraj))
    result.add((ex_ns.Kraj, qb_ns.Concept, sdmx_concept_ns.Kraj))
    result.add((ex_ns.Kraj, RDFS.range, RDFS.Resource))

    result.add((ex_ns.OborPece, RDF.type, RDF.Property))
    result.add((ex_ns.OborPece, RDF.type, qb_ns.DimensionProperty))
    result.add((ex_ns.OborPece, RDFS.label, Literal("OborPece", lang="cs")))
    result.add((ex_ns.OborPece, skos_ns.prefLabel, Literal("OborPece", lang="cs")))
    result.add((ex_ns.OborPece, RDFS.subPropertyOf, sdmx_dimension_ns.OborPece))
    result.add((ex_ns.OborPece, qb_ns.Concept, sdmx_concept_ns.OborPece))
    result.add((ex_ns.OborPece, RDFS.range, RDFS.Resource))

    result.add((ex_ns.PocetPoskytovatelu, RDF.type, RDF.Property))
    result.add((ex_ns.PocetPoskytovatelu, RDF.type, qb_ns.DimensionProperty))
    result.add((ex_ns.PocetPoskytovatelu, RDFS.label, Literal("PocetPoskytovatelu", lang="cs")))
    result.add((ex_ns.PocetPoskytovatelu, skos_ns.prefLabel, Literal("PocetPoskytovatelu", lang="cs")))
    result.add((ex_ns.PocetPoskytovatelu, RDFS.subPropertyOf, sdmx_measure_ns.PocetPoskytovatelu))
    result.add((ex_ns.PocetPoskytovatelu, qb_ns.Concept, sdmx_concept_ns.PocetPoskytovatelu))
    result.add((ex_ns.PocetPoskytovatelu, RDFS.range, RDFS.Resource))

    counter = 0
    for record in content:
        resource = URIRef(f"{ex_ns}observation-{counter:03}")
        result.add((resource, RDF.type, qb_ns.Observation))
        result.add((resource, qb_ns.DataSet, qb_ns.dataCubeInstance))
        result.add((resource, ex_ns.Okres, Literal(record["Okres"])))
        result.add((resource, ex_ns.Kraj, Literal(record["Kraj"])))
        result.add((resource, ex_ns.OborPece, Literal(record["OborPece"])))
        result.add((resource, ex_ns.PocetPoskytovatelu, Literal(record["PocetPoskytovateluPece"])))
        counter = counter + 1

    return result


def print_rdf_as_trig(graph: Graph):
    print(graph.serialize(format="trig"))


if __name__ == "__main__":
    main()
