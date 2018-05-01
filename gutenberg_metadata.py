import rdflib

graph=rdflib.Graph()
graph.load('/newvolume/catalog/cache/epub/3115/pg3115.rdf')

with open('3115_catalog.txt', 'w') as the_file:
    for subj, pred, obj in graph:
        the_file.write("Subject: " + subj + "\n")
        the_file.write("Predicate: " + pred + "\n")
        the_file.write("Object: " + obj + "\n")

