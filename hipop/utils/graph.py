from typing import List, Set, Dict
import networkx
import pddl
import sys

INFTY = sys.maxsize

def subtypes_closure(types: List[pddl.Type]) -> Dict[str, Set[str]]:
    graph = networkx.DiGraph()
    graph.add_node('object')
    for typ in types:
        graph.add_edge(typ.type, typ.name)
        graph.add_edge('object', typ.type)
    closure = networkx.transitive_closure(graph)
    return {t: frozenset(closure.successors(t)) for t in closure}
