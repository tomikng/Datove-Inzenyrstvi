import os
import sys

from rdflib import Graph
from rdflib.plugins.sparql import prepareQuery

# Define path to data cube file
data_cube_file = sys.argv[1]

# Define path to directory containing constraint queries
constraint_directory = "constraints/"

# Load data cube file into graph
g = Graph()
g.parse(data_cube_file, format="turtle")

not_ok = 0
ok = 0
failed = []

# Validate data cube against each constraint query in the directory
for constraint_file in os.listdir(constraint_directory):
    constraint_path = os.path.join(constraint_directory, constraint_file)
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
