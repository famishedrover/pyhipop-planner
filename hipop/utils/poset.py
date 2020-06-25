from typing import TypeVar, Generic, Iterator, List, Dict, Set, Union
import networkx
import logging

T = TypeVar('T')
LOGGER = logging.getLogger(__name__)

class Poset(Generic[T]):

    def __init__(self, graph: networkx.DiGraph = networkx.DiGraph()):
        self.__graph = graph
        self.close()

    @property
    def _edges(self):
        return self.__graph.edges

    def add(self, element: T, relation: Iterator[T] = [],
            #label: str = '',
            check_poset: bool = True) -> bool:
        self.__graph.add_node(element)#, label=label)
        for el in relation:
            self.__graph.add_edge(element, el)
        self.__closed = False
        if check_poset:
            return self.is_poset()
        return True

    def add_relation(self, x: T, y: Union[T,List[T]],
                     check_poset: bool = True) -> bool:
        if type(y) is list:
            for el in y:
                if not self.add_relation(x, el, check_poset):
                    return False
        else:
            self.__graph.add_edge(x, y)
            self.__closed = False
            if check_poset:
                return self.is_poset()
            return True

    def is_poset(self):
        return (networkx.is_directed_acyclic_graph(self.__graph)
                and
                networkx.number_of_selfloops(self.__graph) == 0)

    def reduction(self) -> 'Poset[T]':
        """Returns transitive reduction of the poset.

        The transitive reduction of G = (V,E) is a graph G- = (V,E-) such
        that for all v,w in V there is an edge (v,w) in E- if and only if
        (v,w) is in E and there is no path from v to w in G with length
        greater than 1.
        """
        return Poset(networkx.transitive_reduction(self.__graph))

    def reduce(self):
        """Transitively recude this poset."""
        self.__graph = networkx.transitive_reduction(self.__graph)

    def closure(self) -> 'Poset[T]':
        """Returns transitive closure of the poset.

        The transitive closure of G = (V,E) is a graph G+ = (V,E+) such that
        for all v, w in V there is an edge (v, w) in E+ if and only if
        there is a path from v to w in G.
        """
        return Poset(networkx.transitive_closure(self.__graph,
                                                 reflexive=False))

    def close(self):
        """Transitively close this poset."""
        self.__graph = networkx.transitive_closure(self.__graph,
                                                   reflexive=False)
        self.__closed = True

    def cardinality(self) -> int:
        return self.__graph.number_of_nodes()

    def is_less_than(self, x: T, y: T) -> bool:
        """Return True if x is strictly less than y in the poset."""
        if not self.__closed:
            LOGGER.debug("Closing poset before comparing elements")
            self.close()
        return self.__graph.has_edge(x, y)

    def is_greater_than(self, x: T, y: T) -> bool:
        """Return True if x is strictly greater than y in the poset."""
        return self.is_less_than(y, x)

    def is_lequal(self, x: T, y: T) -> bool:
        """Return True if x is less than or equal to y in the poset."""
        return self.is_less_than(x, y) or (not self.is_greater_than(y, x))

    def is_gequal(self, x: T, y: T) -> bool:
        """Return True if x is greater than or equal to y in the poset."""
        return self.is_lequal(y, x)

    def has_bottom(self) -> bool:
        """Return True if the poset has a unique minimal element."""
        ins = self.__graph.in_degree(self.__graph.nodes)
        return len(list(filter(lambda x: x[1] == 0, ins))) == 1

    def has_top(self) -> bool:
        """Return True if the poset has a unique maximal element."""
        outs = self.__graph.out_degree(self.__graph.nodes)
        return len(list(filter(lambda x: x[1] == 0, outs))) == 1

    def is_bounded(self) -> bool:
        """Return True if the poset is bounded, and False otherwise."""
        return self.has_bottom() and self.has_top()

    def maximal_elements(self) -> Iterator[T]:
        """Return the list of the maximal elements of the poset."""
        outs = self.__graph.out_degree(self.__graph.nodes)
        return map(lambda x: x[0], filter(lambda x: x[1] == 0, outs))

    def minimal_elements(self) -> Iterator[T]:
        """Return the list of the minimal elements of the poset."""
        ins = self.__graph.in_degree(self.__graph.nodes)
        return map(lambda x: x[0], filter(lambda x: x[1] == 0, ins))

    def top(self) -> T:
        """Return the top element of the poset, if it exists."""
        maxs = list(self.maximal_elements())
        if len(maxs) == 1:
            return maxs[0]
        else:
            return None

    def bottom(self) -> T:
        """Return the bottom element of the poset, if it exists."""
        mins = list(self.minimal_elements())
        if len(mins) == 1:
            return mins[0]
        else:
            return None

    def topological_sort(self) -> Iterator[T]:
        return networkx.topological_sort(self.__graph)

    def graphviz_string(self, reduce: bool = False) -> str:
        if reduce:
            self.reduce()
        return ("digraph {\n"
                + "\n".join(map(lambda x: f"{x[0]} -> {x[1]};", self.__graph.edges))
                + "}")

    @classmethod
    def subtypes_closure(cls, types: List[T]) -> Dict[str, Set[str]]:
        poset = cls(networkx.DiGraph())
        poset.add('object', [typ.type for typ in types])
        for typ in types:
            poset.add(typ.type, [typ.name])
        poset.close()
        return {n: frozenset(poset.__graph.successors(n)) for n in poset.__graph}
