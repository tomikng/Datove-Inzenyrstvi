import argparse
import sys

from rdflib import Graph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import RDFS, FOAF, XSD, PROV, RDF

NSP = Namespace("https://example.com/provenance#")
NSR = Namespace("https://example.com/resources/")
dataset = URIRef(
    'https://www.czso.cz/documents/10180/165603907/13007221n01.xlsx/65344c95-18ed-4020-a866-868ba56e52e5?version=1.2')
creator = URIRef('https://github.com/tomikng')
org = URIRef('https://mff.cuni.cz')
ministry_of_health = URIRef('https://www.czso.cz')


def add_entitities(result):
    data_cube = NSR.Population

    result.add((data_cube, RDF.type, PROV.Entity))
    result.add((data_cube, RDFS.label, Literal("Population data cube", lang="en")))
    result.add((data_cube, PROV.wasGeneratedBy, NSP.PopulationScript))
    result.add((data_cube, PROV.wasDerivedFrom, dataset))
    result.add((data_cube, PROV.wasAttributedTo, creator))

    result.add((dataset, RDF.type, PROV.Entity))
    result.add((dataset, PROV.wasAttributedTo, NSP.CZSO))
    result.add((dataset, RDFS.label, Literal("Population Dataset", lang='en')))


def add_agents(result):
    result.add((creator, RDF.type, PROV.Agent))
    result.add((creator, RDF.type, PROV.Person))
    result.add((creator, FOAF.name, Literal("Hai Hung Nguyen")))
    result.add((creator, PROV.actedOnBehalfOf, org))

    result.add((org, RDF.type, PROV.Agent))
    result.add((org, RDF.type, PROV.Organization))
    result.add((org, FOAF.name, Literal("MFF UK")))

    result.add((ministry_of_health, RDF.type, PROV.Agent))
    result.add((ministry_of_health, RDF.type, PROV.Organization))
    result.add((ministry_of_health, FOAF.name, Literal("Czech statistical bureau", lang='en')))


def add_activities(result):
    activity = NSP.CreatePopulationDataCube
    result.add((activity, RDF.type, PROV.Activity))
    result.add((activity, PROV.used, NSP.PopulationDataset))
    result.add((activity, PROV.wasAssociatedWith, creator))
    result.add((activity, PROV.startedAtTime, Literal("2023-04-011T11:49:00", datatype=XSD.dateTime)))
    result.add((activity, PROV.endedAtTime, Literal("2023-04-011T11:55:00", datatype=XSD.dateTime)))

    bnode = BNode()
    result.add((activity, PROV.qualifiedUsage, bnode))
    result.add((bnode, RDF.type, PROV.Usage))
    result.add((bnode, PROV.entity, dataset))
    result.add((bnode, PROV.hadRole, NSR.role))


def add_roles(result):
    result.add((NSR.role, RDF.type, PROV.Role))


def crate_prov_graph() -> Graph:
    result = Graph(bind_namespaces="rdflib")

    add_entitities(result)
    add_agents(result)
    add_activities(result)
    add_roles(result)

    return result


def create_prov_document(output_path: str):
    data = crate_prov_graph()
    data.serialize(format="ttl", destination=output_path, encoding='UTF-8')


def get_args():
    parser = argparse.ArgumentParser(description='Create population data cube provenance')

    parser.add_argument('-o', '--out', dest='out', default=sys.stdout.buffer, type=argparse.FileType('wb'),
                        help='Specify the file path to write the provenance.')

    args = parser.parse_args()
    return args


def main():
    args = get_args()
    create_prov_document(args.out)


if __name__ == '__main__':
    main()
