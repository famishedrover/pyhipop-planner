import sys
import random
import logging
from collections import defaultdict
from copy import deepcopy, copy
from sortedcontainers import SortedKeyList

from ..problem.problem import Problem
from ..problem.operator import GroundedTask
from .plan import HierarchicalPartialPlan
from ..utils.logic import Literals

LOGGER = logging.getLogger(__name__)

class POP():

    def __init__(self, problem,
                 no_duplicate_search: bool = False,
                 poset_inc_impl: bool = True):
        self.__problem = problem
        self.__nds = no_duplicate_search
        self.__poset_inc_impl = poset_inc_impl
        self.__stop_planning = False
        # todo: we can initialize different OpenLists using parameters and heuristic functions
        self.OPEN = SortedKeyList(key=lambda x: x.heuristic())

    @property
    def problem(self):
        return self.__problem

    @property
    def empty_openlist(self):
        return len(self.OPEN) < 1

    def stop(self):
        self.__stop_planning = True

    def get_best_partialPlan(self) -> HierarchicalPartialPlan:
        """
        Returns the best partial plan from the OPEN list
        according to an heuristic.
        Actually, this heuristic is random.
        :param flaws: the set of flaws
        :return: selected flaw
        """
        return self.OPEN[0]

    @staticmethod
    def print_plan(plan):
        import io
        from hipop.utils.io import output_ipc2020_hierarchical
        out_plan = io.StringIO()
        output_ipc2020_hierarchical(plan, out_plan)

    def solve(self, problem):
        """
         Searches for a plan that accomplishes tasks in state.
        :param problem: problem to solve
        :return: the plan
        """
        self.__stop_planning = False
        plan = HierarchicalPartialPlan(self.problem, init=True)
        plan.add_task(problem.goal_task)
        result = self.seek_plan(None, plan)
        return result

    def seek_plan(self, state, pplan) -> HierarchicalPartialPlan:
        if self.__stop_planning: return None

        LOGGER.debug("state: %s", state)
        LOGGER.debug("partial_plan: %s", pplan)
        LOGGER.debug("initial partial plan: %s", list(pplan.sequential_plan()))

        # Initial partial plan
        self.OPEN.add(pplan)
        CLOSED = list()

        # main search loop
        while not self.empty_openlist and not self.__stop_planning:

            current_pplan = self.get_best_partialPlan()

            if current_pplan in CLOSED:
                self.OPEN.remove(current_pplan)
                LOGGER.debug("removing already visited plan")
                continue

            if not current_pplan.has_flaws:
                # if we cannot find an operator with flaws, then the plan is good
                LOGGER.warning("returning plan: %s", list(current_pplan.sequential_plan()))
                return current_pplan

            current_pplan.has_pending_flaws
            LOGGER.info("Current plan has {} flaws ({} : {} : {})".format(len(current_pplan.pending_abstract_flaws) + len(current_pplan.pending_open_links) + len(current_pplan.pending_threats),
                                                                           len(current_pplan.pending_abstract_flaws),
                                                                           len(current_pplan.pending_open_links),
                                                                           len(current_pplan.pending_threats) ))
            # Todo: we should pop from a flaws list
            #   ordered following an heuristic value.
            current_flaw = current_pplan.get_best_flaw()
            LOGGER.debug("resolver candidate: %s", current_flaw)

            close_plan = not current_pplan.has_pending_flaws

            resolvers = []
            if current_flaw in current_pplan.abstract_flaws:
                resolvers = list(current_pplan.resolve_abstract_flaw(current_flaw))
                for r in resolvers:
                    LOGGER.debug("resolver: %s", r)
                if not resolvers:
                    close_plan = True
                    LOGGER.debug("Abstract flaw without resolution")

            elif current_flaw in current_pplan.threats:
                resolvers = next((t[1] for t in current_pplan.pending_threats if t[0] == current_flaw), None)
                assert(current_flaw == (current_pplan.pending_threats.pop(0))[0])
                if not resolvers:
                    close_plan = True
                    LOGGER.debug("Threat without resolution")

            elif current_flaw in current_pplan.open_links:
                resolvers = list(current_pplan.resolve_open_link(current_flaw))
                if not resolvers and len(current_pplan.pending_abstract_flaws) == 0 and len(current_pplan.pending_threats) == 0:
                    close_plan = True
                    LOGGER.debug("OpenLink without resolution")

            i = 0
            for r in resolvers:
                LOGGER.debug("new partial plan: %s", r)
                i += 1
                if not r in CLOSED:
                    self.OPEN.add(r)
            LOGGER.debug("   just added %d plans to open lists", i)

            if close_plan:
                CLOSED.append(current_pplan)
                self.OPEN.remove(current_pplan)

            LOGGER.info("Open List size: %d", len(self.OPEN))
            LOGGER.info("Closed List size: %d", len(CLOSED))
        # end while
        LOGGER.warning("nothing leads to solution")
        return None
