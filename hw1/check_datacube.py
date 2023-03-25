import os
import argparse
from rdflib import Graph
from rdflib.plugins.sparql import prepareQuery

# Create the argument parser
parser = argparse.ArgumentParser(description="Validate a data cube against a set of constraints.")
parser.add_argument("data_cube_file", help="path to the data cube file in Turtle format")
parser.add_argument("--constraint-directory", "-c", default="constraints/", help="path to the directory containing constraint queries")
args = parser.parse_args()

# Load data cube file into graph
g = Graph()
g.parse(args.data_cube_file, format="turtle")

not_ok = 0
ok = 0
failed = []

# Validate data cube against each constraint query in the directory
for constraint_file in os.listdir(args.constraint_directory):
    constraint_path = os.path.join(args.constraint_directory, constraint_file)
    with open(constraint_path) as f:
        constraint_query = prepareQuery(f.read())

    violations = g.query(constraint_query)
    if violations:
        failed.append(constraint_file)
        not_ok += 1
        # print(f"Constraint {constraint_file} violated!")
    else:
        ok += 1
        # print(f"Constraint {constraint_file} satisfied!")

if len(failed) > 0:
    for fail in failed:
        print(f"Constraint {fail} violated!")

print(f"Passed: {ok}, Failed: {not_ok}, Total: {ok + not_ok}")
