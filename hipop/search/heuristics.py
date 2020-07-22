from abc import ABC, abstractmethod
from typing import Any, Union
import math
import logging
from collections import defaultdict

from ..utils.logic import Literals

LOGGER = logging.getLogger(__name__)

class Heuristic(ABC):
    def heuristic(self, element: Any):
        return 0

class HAdd(Heuristic):

    def __init__(self, actions, init):
        """H_add computation from V. Vidal, 'YAHSP2: Keep It Simple, Stupid', IPC2011."""

        self.__hadd = dict()
        update = dict()
        literals = Literals.literals()

        lit_in_pre = defaultdict(list)
        pres = defaultdict(list)
        adds = defaultdict(list)
        costs = dict()

        for action in actions:
            aname = str(action)
            self.__hadd[aname] = math.inf
            pos, neg = action.support
            for lit in pos:
                lit_in_pre[lit].append(aname)
            for lit in neg:
                lit_in_pre[lit].append(aname)
            adds[aname] = list(action.effect[0])
            pres[aname] = list(pos) + list(neg)
            costs[aname] = action.cost
            update[aname] = (len(pres[aname]) == 0)

        for atom in literals:
            if atom in init:
                self.__hadd[atom] = 0
                for action in lit_in_pre[atom]:
                    update[action] = True
            else:
                self.__hadd[atom] = math.inf

        loop = True
        while loop:
            loop = False
            for action in actions:
                aname = str(action)
                #LOGGER.debug("action %s must be updated? %s", aname, update[aname])
                if update[aname]:
                    update[aname] = False
                    c = sum(self.__hadd[p] for p in pres[aname])
                    if c < self.__hadd[aname]:
                        #LOGGER.debug("new h_add for action %s: %d", aname, c)
                        self.__hadd[aname] = c
                        for p in adds[aname]:
                            g = c + costs[aname]
                            if g < self.__hadd[p]:
                                #LOGGER.debug("new h_add for literal %d: %d", p, g)
                                self.__hadd[p] = g
                                for action in lit_in_pre[p]:
                                    loop = True
                                    update[action] = True
        LOGGER.info("h_add computed for %d elements", len(self.__hadd))
        for lit, hadd in self.__hadd.items():
            if type(lit) == int:
                LOGGER.debug("h_add([%d]%s) = %s", lit, Literals.lit_to_predicate(lit), hadd)

    def heuristic(self, element: int):
        return self.__hadd[element]
        
