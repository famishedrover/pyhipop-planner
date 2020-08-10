from typing import Dict, List, Set, Iterator, Tuple
import logging
import pddl
from collections import defaultdict

from ..utils.poset import Poset

LOGGER = logging.getLogger(__name__)


class Objects:
    """Objects of the problem.

    Objects are sorted by (super) types.

    :param problem: the PDDL problem
    :param domain: the PDDL domain
    """
    def __init__(self, problem: pddl.Problem, domain: pddl.Domain):
        
        self.__subtypes_closure(domain.types)
        LOGGER.info("Types: %d", len(self.__types_hierarchy))
        LOGGER.debug("Types: %s", self.__types_hierarchy.nodes)

        self.__objects_per_type = defaultdict(set)
        objects = set()
        for obj in domain.constants:
            self.__objects_per_type[obj.type].add(obj.name)
            objects.add(obj.name)
        for obj in problem.objects:
            self.__objects_per_type[obj.type].add(obj.name)
            objects.add(obj.name)
        for t, subt in self.__types_subtypes.items():
            for st in subt:
                self.__objects_per_type[t] |= self.__objects_per_type[st]
        LOGGER.info("Objects: %d", len(objects))
        LOGGER.debug("Objects: %s", objects)
        LOGGER.debug("Objects per type:")
        for typ, objs in self.__objects_per_type.items():
            LOGGER.debug('- %s: %s', typ, objs)

        self.__objects = list(objects)
        for typ, objs in self.__objects_per_type.items():
            self.__objects_per_type[typ] = list(objs)

    def __iter__(self):
        return self.__objects.__iter__()

    @property
    def types(self) -> Iterator[str]:
        """Get all types."""
        return self.__objects_per_type.keys()

    def per_type(self, objtype: str = 'object') -> Iterator[str]:
        """Get all objects of a given type.

        :param objtype: the given type
        """
        return self.__objects_per_type[objtype].__iter__()

    def write_dot(self, filename: str):
        self.__types_hierarchy.write_dot(filename)

    def __subtypes_closure(self, types: List[pddl.Type]) -> Dict[str, Set[str]]:
        """Computes the transitive closure of types hierarchy."""
        self.__types_hierarchy = Poset()
        for typ in types:
            self.__types_hierarchy.add_relation(typ.type, typ.name)
            self.__types_hierarchy.add_relation('object', typ.type)
        self.__types_hierarchy.close()
        graph = self.__types_hierarchy.graph
        self.__types_subtypes = {n: frozenset(graph.successors(n)) for n in graph}
