import argparse
import sys

from rdflib import Graph, Literal, Namespace, URIRef, BNode
from rdflib.namespace import RDFS, FOAF, XSD, PROV, RDF

NSP = Namespace("https://example.com/provenance#")
NSR = Namespace("https://example.com/resources/")
dataset = URIRef(
    'https://opendata.mzcr.cz/data/nrpzs/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv')
creator = URIRef('https://github.com/tomikng')
org = URIRef('https://mff.cuni.cz')
ministry_of_health = URIRef('https://www.mzcr.cz')


def add_entitities(result):
    data_cube = NSR.CareProviders

    result.add((data_cube, RDF.type, PROV.Entity))
    result.add((data_cube, RDFS.label, Literal("Care providers data cube", lang="en")))
    result.add((data_cube, PROV.wasGeneratedBy, NSP.CareProvidersCubeScript))
    result.add((data_cube, PROV.wasDerivedFrom, dataset))
    result.add((data_cube, PROV.wasAttributedTo, creator))

    result.add((dataset, RDF.type, PROV.Entity))
    result.add((dataset, PROV.wasAttributedTo, ministry_of_health))
    result.add((dataset, RDFS.label, Literal("Care Providers Dataset", lang='en')))


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
    result.add((ministry_of_health, FOAF.name, Literal("Ministry of health", lang='en')))


def add_activities(result):
    activity = NSP.CreateCareProvidersDataCube
    result.add((activity, RDF.type, PROV.Activity))
    result.add((activity, PROV.used, NSP.CareProvidersDataset))
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
    parser = argparse.ArgumentParser(description='Create care providers data cube provenance')

    parser.add_argument('-o', '--out', dest='out', type=argparse.FileType('wb'),
                        help='Specify the file path to write the provenance.')

    args = parser.parse_args()
    return args


def main():
    args = get_args()
    create_prov_document(args.out)


if __name__ == '__main__':
    main()
