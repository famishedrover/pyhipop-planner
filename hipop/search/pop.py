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

    def __init__(self, problem):
        self.__problem = problem
        self.__stop_planning = False
        # todo: we can initialize different OpenLists using parameters and heuristic functions
        self.OPEN = SortedKeyList(key=lambda x: x.f)

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
        plan = HierarchicalPartialPlan(self.problem, init=True, goal=True, poset_inc_impl=True)
        plan.add_task(problem.goal_task)
        result = self.seek_plan(None, plan)
        if result:
            result.write_dot("plan.dot")
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
            if LOGGER.isEnabledFor(logging.DEBUG):
                current_pplan.write_dot(f"current-plan.dot")
            LOGGER.debug("current plan id: %s (cost function: %s)", id(current_pplan), current_pplan.f)

            if not current_pplan.has_flaws:
                # if we cannot find an operator with flaws, then the plan is good
                LOGGER.warning("returning plan: %s", list(current_pplan.sequential_plan()))
                return current_pplan

            if (current_pplan in CLOSED) or (not current_pplan.compute_flaw_resolvers()):
                self.OPEN.remove(current_pplan)
                LOGGER.debug("removing plan")
                continue

            LOGGER.info("Current plan has {} flaws ({} : {} : {})".format(len(current_pplan.pending_abstract_flaws) + len(current_pplan.pending_open_links) + len(current_pplan.pending_threats),
                                                                           len(current_pplan.pending_abstract_flaws),
                                                                           len(current_pplan.pending_open_links),
                                                                           len(current_pplan.pending_threats) ))
            # Todo: we should pop from a flaws list
            #   ordered following an heuristic value.
            current_flaw = current_pplan.get_best_flaw()
            LOGGER.debug("resolver candidate: %s", current_flaw)

            close_plan = not current_pplan.has_pending_flaws

            resolvers = current_pplan.resolvers(current_flaw)
            i = 0
            for r in resolvers:
                i += 1
                LOGGER.debug("resolver: %s", id(r))
                if LOGGER.isEnabledFor(logging.DEBUG):
                    r.write_dot(f"plan-{id(r)}.dot")
                if r in CLOSED:
                    LOGGER.debug("plan %s already in CLOSED set", id(r))
                else:
                    self.OPEN.add(r)
            LOGGER.debug("   just added %d plans to open lists", i)

            if close_plan:
                LOGGER.debug("closing current plan")
                CLOSED.append(current_pplan)
                self.OPEN.remove(current_pplan)

            LOGGER.info("Open List size: %d", len(self.OPEN))
            LOGGER.info("Closed List size: %d", len(CLOSED))
        # end while
        LOGGER.warning("nothing leads to solution")
        return None
