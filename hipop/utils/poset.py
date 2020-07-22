from typing import TypeVar, Generic, Iterator, List, Dict, Set, Union, Optional
from collections import defaultdict
import networkx
import logging
import networkx.algorithms.isomorphism as nxiso
import networkx.drawing.nx_pydot as nx_pydot

T = TypeVar('T')
LOGGER = logging.getLogger(__name__)

class Poset(Generic[T]):

    def __init__(self, graph: networkx.DiGraph = networkx.DiGraph()):
        self._graph = graph#.copy()
        self.__closed = False
        self.close()

    def __eq__(self, poset):
        if (len(self._graph.edges) != len(poset._graph.edges)):
            return False
        if (len(self._graph.nodes) != len(poset._graph.nodes)):
            return False
        iso = networkx.is_isomorphic(self._graph, poset._graph,
                                     node_match=nxiso.categorical_node_match('operator', ""),
                                     edge_match=nxiso.categorical_edge_match('relation', set()))
        return iso

    def subposet(self, nodes):
        return Poset(self._graph.subgraph(nodes))

    @property
    def nodes(self):
        return self._graph.nodes

    @property
    def edges(self):
        return self._graph.edges

    def add(self, element: T, operator: str = "") -> bool:
        LOGGER.debug("adding node %s", element)
        self._graph.add_node(element, operator=operator)
        return True

    def remove(self, element: T):
        LOGGER.debug("remove %s", element)
        self._graph.remove_node(element)

    def _add_edge(self, x: T, y: T, relation: str):
        if self._graph.has_edge(x, y):
            rel = self._graph[x][y]['relation']
            rel.add(relation)
            LOGGER.debug("update edge %s %s relation %s", x, y, rel)
        else:
            LOGGER.debug("adding edge %s %s relation %s", x, y, relation)
            if isinstance(relation, set):
                rel = relation
            else:
                rel = set()
                rel.add(relation)
            self._graph.add_edge(x, y, relation=rel)

    def add_relation(self, x: T, y: Union[T,List[T]],
                     relation: Optional[str] = '<',
                     check_poset: bool = False) -> bool:
        if type(y) is list:
            for el in y:
                if not self.add_relation(x, el, relation, check_poset):
                    return False
        else:
            self._add_edge(x, y, relation)
            self.__closed = False
            if check_poset:
                return self.is_poset()
            return True

    @property
    def poset(self) -> networkx.Graph:
        return self._graph

    def is_poset(self) -> bool:
        return (networkx.is_directed_acyclic_graph(self._graph)
                and
                networkx.number_of_selfloops(self._graph) == 0)

    def reduction(self) -> 'Poset[T]':
        """Returns transitive reduction of the poset.

        The transitive reduction of G = (V,E) is a graph G- = (V,E-) such
        that for all v,w in V there is an edge (v,w) in E- if and only if
        (v,w) is in E and there is no path from v to w in G with length
        greater than 1.
        """
        return super().__init__(networkx.transitive_reduction(self._graph))

    def reduce(self):
        """Transitively recude this poset."""
        self._graph = networkx.transitive_reduction(self._graph)

    def closure(self) -> 'Poset[T]':
        """Returns transitive closure of the poset.

        The transitive closure of G = (V,E) is a graph G+ = (V,E+) such that
        for all v, w in V there is an edge (v, w) in E+ if and only if
        there is a path from v to w in G.
        """
        return super().__init__(networkx.transitive_closure(self._graph,
                                                 reflexive=False))

    def close(self):
        """Transitively close this poset."""
        if not self.__closed:
            self._graph = networkx.transitive_closure(self._graph,
                                                      reflexive=False)
            self.__closed = True

    def cardinality(self) -> int:
        return self._graph.number_of_nodes()

    def is_less_than(self, x: T, y: T) -> bool:
        """Return True if x is strictly less than y in the poset."""
        if not self.__closed:
            LOGGER.debug("Closing poset before comparing elements")
            self.close()
        return self._graph.has_edge(x, y)

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
        ins = self._graph.in_degree(self._graph.nodes)
        return len(list(filter(lambda x: x[1] == 0, ins))) == 1

    def has_top(self) -> bool:
        """Return True if the poset has a unique maximal element."""
        outs = self._graph.out_degree(self._graph.nodes)
        return len(list(filter(lambda x: x[1] == 0, outs))) == 1

    def is_bounded(self) -> bool:
        """Return True if the poset is bounded, and False otherwise."""
        return self.has_bottom() and self.has_top()

    def maximal_elements(self) -> Iterator[T]:
        """Return the list of the maximal elements of the poset."""
        outs = self._graph.out_degree(self._graph.nodes)
        return map(lambda x: x[0], filter(lambda x: x[1] == 0, outs))

    def minimal_elements(self) -> Iterator[T]:
        """Return the list of the minimal elements of the poset."""
        ins = self._graph.in_degree(self._graph.nodes)
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

    def topological_sort(self, nodes=None) -> Iterator[T]:
        if nodes is None:
            return networkx.topological_sort(self._graph)
        else:
            LOGGER.debug("top. sort on nodes %s", nodes)
            subgraph = self._graph.subgraph(nodes)
            return networkx.topological_sort(subgraph)

    def graphviz_string(self, reduce: bool = False) -> str:
        if reduce:
            self.reduce()
        return ("digraph {\n"
                + "\n".join(map(lambda x: f"{x[0]} -> {x[1]};", self._graph.edges))
                + "}")

    def write_dot(self, filename):
        nx_pydot.write_dot(self._graph, filename)

    @classmethod
    def subtypes_closure(cls, types: List[T]) -> Dict[str, Set[str]]:
        poset = cls(networkx.DiGraph())
        poset.add('object', [typ.type for typ in types])
        for typ in types:
            poset.add(typ.type)
            poset.add_relation(typ.type, typ.name)
        poset.close()
        return {n: frozenset(poset._graph.successors(n)) for n in poset._graph}

class IncrementalPosetError(Exception):
    def __init__(self, message):
        self.__message = message
    @property
    def message(self):
        return self.__message
    def __str__(self):
        return self.__message

class IncrementalPoset(Poset):

    def __init__(self):
        Poset.__init__(self)
        self.__L = defaultdict(lambda: 0)
        self.__Paths = defaultdict(lambda: defaultdict(lambda: 0))

    @property
    def L(self):
        return self.__L

    def remove(self, element: T):
        #LOGGER.debug("inc remove %s", element)
        for u in self._graph.successors(element):
            self.__L[u] = max(self.__L[v] for v in self._graph.predecessors(u)) + 1
            self.__follow(u, [])
        self._graph.remove_node(element)

    def __follow(self, u: T, path: List[T]):
        if u in path:
            LOGGER.debug("Cycle detected in poset: %s %s", u, path)
            return False
        for v in self._graph.successors(u):
            if self.__L[u] < self.__L[v]:
                pass
            else:
                self.__L[v] = self.__L[u] + 1
                #OGGER.debug("updating L[%s] = %d", v, self.__L[v])
                if not self.__follow(v, path + [u]):
                    return False
        return True

    def _add_edge(self, x: T, y: T, relation: str) -> bool:
        LOGGER.debug("add edge %s->%s", x, y)
        if self.__L[x] < self.__L[y]:
            Poset._add_edge(self, x, y, relation)
            return True
        else:
            self.__L[y] = self.__L[x] + 1
            #LOGGER.debug("updating L[%s] = %d", y, self.__L[y])
            if self.__follow(y, [x]):
                Poset._add_edge(self, x, y, relation)
            return True
        return False

    def __propagate_path(self, u: T, v: T):
        self.__Paths[u][v] += 1
        LOGGER.debug("propagating path when inserting %s->%s", u, v)
        From = {i: self.__Paths[i][u] for i in self._graph.nodes}
        LOGGER.debug("From: %s", From)
        To = {j: self.__Paths[v][j] for j in self._graph.nodes}
        LOGGER.debug("To: %s", To)
        for i in self._graph.nodes:
            if i == v: continue
            for j in self._graph.nodes:
                if j == v: continue
                if (From[i] == 0) and (To[j] == 0):
                    continue
                self.__Paths[i][j] += (#self.__Paths[i][v] * To[j]
                                       #+ From[i] * self.__Paths[v][j] +
                                       From[i] * To[j])
                LOGGER.debug("updating Paths[%s][%s] = %d", i, j, self.__Paths[i][j])
        for i in self._graph.nodes:
            if i == v: continue
            if From[i] == 0: continue
            self.__Paths[i][v] += From[i]
            LOGGER.debug("updating Paths[%s][%s] = %d", i, v, self.__Paths[i][v])
        for j in self._graph.nodes:
            if j == v: continue
            if To[j] == 0: continue
            self.__Paths[u][j] += To[j]
            LOGGER.debug("updating Paths[%s][%s] = %d", v, j, self.__Paths[u][j])

    def add_relation(self, x: T, y: Union[T, List[T]],
                     relation: Optional[str] = '<',
                     check_poset: bool = False) -> bool:
        if type(y) is list:
            for el in y:
                if not self.add_relation(x, el, relation, check_poset):
                    return False
            return True
        if self._add_edge(x, y, relation):
            #self.__propagate_path(x, y)
            return True
        return False

    def close(self):
        pass

    def is_poset(self):
        return True

    def is_less_than(self, x: T, y: T) -> bool:
        """Return True if x is strictly less than y in the poset."""
        return networkx.has_path(self._graph, x, y)
        #return self.__Paths[x][y] > 0

    def has_bottom(self) -> bool:
        """Return True if the poset has a unique minimal element."""
        mins = self.minimal_elements()
        return len(mins) == 1

    '''
    def has_top(self) -> bool:
        """Return True if the poset has a unique maximal element."""
        maxs = self.maximal_elements()
        return len(maxs) == 1

    def maximal_elements(self) -> Iterator[T]:
        """Return the list of the maximal elements of the poset."""
        m = max(self.__L.values())
        return set(k for k, v in self.__L.items() if v == m)
    '''

    def minimal_elements(self) -> Iterator[T]:
        """Return the list of the minimal elements of the poset."""
        m = min(self.__L.values())
        return set(k for k, v in self.__L.items() if v == m)

    def topological_sort(self, nodes=None) -> Iterator[T]:
        if nodes is None:
            nodes = self._graph
        sorted_nodes = sorted(self.__L, key=self.__L.get)
        LOGGER.debug("L: %s", self.__L)
        LOGGER.debug("sorted: %s", list(sorted_nodes))
        return filter(lambda x: x in nodes, sorted_nodes)
