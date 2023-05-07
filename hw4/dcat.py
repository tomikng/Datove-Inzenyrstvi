import datetime
import os

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS, RDF, XSD, FOAF

NS = Namespace("https://https://example.com/")
NSR = Namespace("https://https://example.com//resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")


def build_dataset(gr: Graph) -> Graph:
    data_uri = NSR.MeanPopulation2021

    gr.add((data_uri, RDF.type, DCAT.Dataset))
    gr.add((data_uri, DCTERMS.title, Literal("Population 2021", lang="en")))
    gr.add((data_uri, DCTERMS.title, Literal("Obyvatelé v okresech 2021", lang="cs")))
    gr.add((data_uri, DCTERMS.description,
            Literal("Datová kostka obsahující obyvatele podle krajů a okresů v roce 2021", lang="cs")))

    for keyword in ["populace", "okresy", "kraje"]:
        gr.add((data_uri, DCAT.keyword, Literal(keyword, lang="cs")))

    for theme in ["http://eurovoc.europa.eu/4259", "http://eurovoc.europa.eu/3300"]:
        gr.add((data_uri, DCAT.theme, URIRef(theme)))

    gr.add((data_uri, DCTERMS.spatial, URIRef("http://publications.europa.eu/resource/authority/atu/CZE")))

    time_node = BNode()
    gr.add((data_uri, DCTERMS.temporal, time_node))
    gr.add((time_node, RDF.type, DCTERMS.PeriodOfTime))
    gr.add((time_node, DCAT.startDate, Literal(datetime.date(2021, 1, 1), datatype=XSD.date)))
    gr.add((time_node, DCAT.endDate, Literal(datetime.date(2021, 12, 31), datatype=XSD.date)))

    dist_node = NSR.CubeDistribution
    gr.add((data_uri, DCAT.distribution, dist_node))

    gr.add((dist_node, RDF.type, DCAT.Distribution))
    gr.add((dist_node, DCAT.accessURL, URIRef("https://github.com/tomikng/Datove-Inzenyrstvi")))
    gr.add((dist_node, DCAT.downloadURL, URIRef("https://github.com/tomikng/Datove-Inzenyrstvi")))
    gr.add((dist_node, DCAT.mediaType, URIRef("http://www.iana.org/assignments/media-types/text/turtle")))
    gr.add((dist_node, DCTERMS.format, URIRef("http://publications.europa.eu/resource/authority/file-type/RDF_TURTLE")))

    person_node = BNode()
    gr.add((data_uri, DCTERMS.publisher, person_node))
    gr.add((data_uri, DCTERMS.creator, person_node))
    gr.add((person_node, RDF.type, FOAF.Person))
    gr.add((person_node, FOAF.firstName, Literal("Hai Hung")))
    gr.add((person_node, FOAF.lastName, Literal("Nguyen")))

    gr.add((data_uri, DCTERMS.accrualPeriodicity,
            URIRef("http://publications.europa.eu/resource/authority/frequency/IRREG")))

    return gr


def main():
    graph = Graph()

    graph = build_dataset(graph)

    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/dcat_dataset.ttl", "wb") as file:
        graph.serialize(file, "ttl")
        print(f"Success, created {file.name}")


if __name__ == "__main__":
    main()
