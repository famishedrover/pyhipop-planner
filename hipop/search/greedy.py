from typing import Optional
import logging
import math
from collections import deque
import enum
from operator import itemgetter
from sortedcontainers import SortedKeyList

from ..grounding.problem import Problem
from ..plan.plan import HierarchicalPartialPlan
from ..plan.flaws import Threat, OpenLink, AbstractFlaw

LOGGER = logging.getLogger(__name__)

class OpenLinkHeuristic(enum.Enum):
    LIFO = 'lifo'
    SORTED = 'sorted'
    LOCAL = 'local'
    EARLIEST = 'earliest'
    SORTED_EARLIEST = 'sorted-earliest'
    LOCAL_EARLIEST = 'local-earliest'


class GreedySearch:
    def __init__(self, problem: Problem, ol_heuristic: OpenLinkHeuristic = OpenLinkHeuristic.LIFO):
        self.__ol_heuristic = ol_heuristic
        self.__hadd = problem.hadd
        # queue structures
        self.__OPEN = SortedKeyList(key=itemgetter(2))
        self.__CLOSED = list()
        # initial plan
        plan = HierarchicalPartialPlan(problem, init=True)
        if problem.has_root_task():
            root = problem.root_task()
            plan.add_task(root)
        sorted_flaws = self.__sort_flaws(plan)
        self.__OPEN.add((plan, sorted_flaws, 0))
        self.__CLOSED.append(plan)

    def __sort_flaws(self, plan: HierarchicalPartialPlan):
        flaws_queue = SortedKeyList(key=itemgetter(1))

        # First, test OL resolvability:
        for ol in plan.open_links:
            if not plan.is_ol_resolvable(ol):
                return None

        if len(plan.threats) > 0:
            for threat in plan.threats:
                flaws_queue.add((threat, 0))

        else:
            seq_plan = list(map(itemgetter(0), plan.sequential_plan()))
            LOGGER.debug("sorting flaws on %s", seq_plan)
            
            first, second = 0, 0
            max_ol = - math.inf
            for ol in plan.open_links:
                if not plan.has_ol_direct_resolvers(ol): continue

                if self.__ol_heuristic == OpenLinkHeuristic.EARLIEST:
                    first = seq_plan.index(ol.step)
                elif self.__ol_heuristic == OpenLinkHeuristic.LIFO:
                    first = plan.open_links.index(ol)
                elif self.__ol_heuristic == OpenLinkHeuristic.LOCAL or self.__ol_heuristic == OpenLinkHeuristic.LOCAL_EARLIEST:
                    first = - ol.step
                elif self.__ol_heuristic == OpenLinkHeuristic.SORTED or self.__ol_heuristic == OpenLinkHeuristic.SORTED_EARLIEST:
                    first = - self.__hadd(ol.atom)
                elif self.__ol_heuristic == OpenLinkHeuristic.LOCAL_EARLIEST or self.__ol_heuristic == OpenLinkHeuristic.SORTED_EARLIEST:
                    second = seq_plan.index(ol.step)

                max_ol = max(max_ol, first)
                flaws_queue.add((ol, (first, second)))
            
            for s in seq_plan:
                try:
                    i = plan.abstract_flaws.index(s)
                    flaws_queue.add((plan.abstract_flaws[i], (max_ol+1, 0)))
                    break
                except ValueError:
                    pass

        return flaws_queue

    def solve(self,
              output_current_plan: bool = True,
              ) -> HierarchicalPartialPlan:

        # Stats
        revisited = 0
        pruned = 0
        iterations = 0

        while self.__OPEN:

            iterations += 1
            prune = False

            plan, flaws, h = self.__OPEN.pop()

            LOGGER.info("current plan: %d, %d flaws, h=%s", id(plan), len(flaws), h)
            if output_current_plan is not None:
                plan.write_dot('current-plan.dot')

            if not plan.has_flaws():
                LOGGER.info("solution found; search statistics: ")
                LOGGER.info("- iterations: %d", iterations)
                LOGGER.info("- closed: %d", len(self.__CLOSED))
                LOGGER.info("- revisited: %d", revisited)
                LOGGER.info("- pruned: %d", pruned)
                LOGGER.info("- opened: %d", len(self.__OPEN))
                return plan

            LOGGER.info("flaws: AF=%d, OL=%d, Th=%d",
                        len(plan.abstract_flaws),
                        len(plan.open_links),
                        len(plan.threats))

            flaw, rank = flaws.pop(0)
            LOGGER.info("current flaw: %s, key=%s, %s",
                        flaw, rank[0], rank[1])

            if isinstance(flaw, Threat):
                resolvers = list(plan.threat_resolvers(flaw))
                if not resolvers:
                    prune = True

            elif isinstance(flaw, AbstractFlaw):
                resolvers = list(plan.abstract_flaw_resolvers(flaw))
                if not resolvers:
                    prune = True

            elif isinstance(flaw, OpenLink):
                resolvers = list(plan.open_link_resolvers(flaw))
                if not resolvers and not plan.has_open_link_task_resolvers(flaw):
                    prune = True

            LOGGER.debug("Resolvers for flaw %s: %d", 
                            flaw, len(resolvers))
                
            if prune:
                pruned += 1
                LOGGER.debug("pruning...")
                continue

            for r in resolvers:
                if r in self.__CLOSED:
                    LOGGER.debug("resolver already closed")
                    revisited += 1
                else:
                    self.__CLOSED.append(r)
                    sorted_flaws = self.__sort_flaws(r)
                    if sorted_flaws is None:
                        LOGGER.debug("no sorted flaws for plan %d: removing", id(r))
                        pruned += 1
                        continue
                    LOGGER.debug("- new plan %d with %d flaws", id(r), len(sorted_flaws))
                    self.__OPEN.add((r, sorted_flaws, h+1))

            if flaws:
                self.__OPEN.add((plan, flaws, h))

            LOGGER.info("Open List size: %d", len(self.__OPEN))
            LOGGER.info("Closed List size: %d", len(self.__CLOSED))
