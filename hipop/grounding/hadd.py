from typing import Union, Set, Iterator
import math
import logging
import networkx
import networkx.drawing.nx_pydot as pydot
from collections import defaultdict

from .atoms import Atoms
from .operator import GroundedAction

LOGGER = logging.getLogger(__name__)

class HAdd:

    def __init__(self, actions: Iterator[GroundedAction], init: Set[int], fluents: Set[int]):
        self.__hadd = dict()
        self.__parents = dict()
        self.__compute(actions, init, fluents)
        LOGGER.info("h_add computed for %d elements", len(self.__hadd))

    def write_dot(self, filename: str = "hadd-graph.dot"):
        graph = networkx.DiGraph()
        lit_to_pred = Atoms.atom_to_predicate
        self.__hadd['__init'] = 0
        for child, parent in self.__parents.items():
            if type(parent) == list:
                for p in parent:
                    graph.add_edge(f"{p} {lit_to_pred(p)}", child, label=self.__hadd[p])
            else:
                graph.add_edge(parent, f"{child} {lit_to_pred(child)}", label=self.__hadd[child])
        pydot.write_dot(graph, filename)

    def __compute(self, actions: Iterator[GroundedAction], init: Set[int], fluents: Set[int]):
        """H_add computation from V. Vidal, 'YAHSP2: Keep It Simple, Stupid', IPC2011."""

        literals = list(fluents)
        update = dict()
        lit_in_pre = defaultdict(list)
        pres = defaultdict(list)
        adds = defaultdict(list)
        costs = dict()

        for action in actions:
            aname = str(action)
            self.__hadd[aname] = math.inf
            pos, _ = action.support
            for lit in pos:
                lit_in_pre[lit].append(aname)
            adds[aname] = list(action.effect[0])
            pres[aname] = list(pos)
            costs[aname] = action.cost
            update[aname] = (len(pres[aname]) == 0)
            if update[aname]:
                self.__parents[aname] = [aname]

        for atom in literals:
            if atom in init:
                self.__hadd[atom] = 0
                for action in lit_in_pre[atom]:
                    update[action] = True
                self.__parents[atom] = '__init'
            else:
                self.__hadd[atom] = math.inf

        loop = True
        while loop:
            loop = False
            for action in actions:
                aname = str(action)
                if update[aname]:
                    update[aname] = False
                    c = sum(self.__hadd[p] for p in pres[aname])
                    if c < self.__hadd[aname]:
                        self.__hadd[aname] = c
                        for p in adds[aname]:
                            g = c + costs[aname]
                            if g < self.__hadd[p]:
                                self.__hadd[p] = g
                                for action in lit_in_pre[p]:
                                    loop = True
                                    update[action] = True
                                self.__parents[p] = aname
                        self.__parents[aname] = pres[aname] if pres[aname] else [aname]

    def __call__(self, element: Union[int, str]) -> int:
        return self.__hadd[element]
