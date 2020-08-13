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


class GreedySearch:
    def __init__(self, problem: Problem):
        self.__problem = problem
        # queue structures
        self.__OPEN = SortedKeyList(key=itemgetter(2))
        self.__CLOSED = list()
        # initial plan
        plan = HierarchicalPartialPlan(problem, init=True)
        if self.__problem.has_root_task():
            root = self.__problem.root_task()
            plan.add_task(root)
        sorted_flaws = self.__sort_flaws(plan)
        self.__OPEN.add((plan, sorted_flaws, 0))
        self.__CLOSED.append(plan)

    def __sort_flaws(self, plan: HierarchicalPartialPlan):
        flaws_queue = SortedKeyList(key=itemgetter(1))
        seq_plan = list(map(itemgetter(0), plan.sequential_plan()))
        LOGGER.debug("sorting flaws on %s", seq_plan)

        if len(plan.threats) > 0:
            for threat in plan.threats:
                flaws_queue.add((threat, seq_plan.index(threat.step)))

        else:
            for ol in plan.open_links:
                i = seq_plan.index(ol.step)
                flaws_queue.add((ol, i))
            for af in plan.abstract_flaws:
                i = seq_plan.index(af.step)
                flaws_queue.add((af, i))

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
            if output_current_plan:
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
            LOGGER.info("current flaw: %s, rank=%d",
                        flaw, rank)

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
                continue

            for r in resolvers:
                if r in self.__CLOSED:
                    LOGGER.debug("resolver already closed")
                    revisited += 1
                else:
                    sorted_flaws = self.__sort_flaws(r)
                    LOGGER.debug("- new plan %d with %d flaws", id(r), len(sorted_flaws))
                    self.__OPEN.add((r, sorted_flaws, h+1))
                    self.__CLOSED.append(r)

            if flaws:
                self.__OPEN.add((plan, flaws, h))

            LOGGER.info("Open List size: %d", len(self.__OPEN))
            LOGGER.info("Closed List size: %d", len(self.__CLOSED))
