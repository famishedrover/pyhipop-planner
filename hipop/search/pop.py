import sys
import logging
from collections import defaultdict
from copy import deepcopy, copy

from ..problem.problem import Problem
from ..problem.operator import GroundedTask
from .plan import HierarchicalPartialPlan
from ..utils.logic import Literals

LOGGER = logging.getLogger(__name__)

class POP():

    def __init__(self, problem,
                 no_duplicate_search: bool = False,
                 hierarchical_plan: bool = True,
                 poset_inc_impl: bool = True):
        self.__problem = problem
        self.__nds = no_duplicate_search
        self.__hierarchical = hierarchical_plan
        self.__poset_inc_impl = poset_inc_impl
        self.__stop_planning = False

    @property
    def problem(self):
        return self.__problem

    def stop(self):
        self.__stop_planning = True

    def print_plan(self, plan):
        import io
        from hipop.utils.io import output_ipc2020_hierarchical
        out_plan = io.StringIO()
        output_ipc2020_hierarchical(plan, out_plan)

    def find_plan(self, state, tasks):
        """
         Searches for a plan that accomplishes tasks in state.
         Basically performs a DFS.
        :param state: Initial state of the search
        :return: the plan
        """
        self.__stop_planning = False

        if self.__hierarchical:
            plan = HierarchicalPartialPlan(self.problem,
                                           init=False,
                                           poset_inc_impl=self.__poset_inc_impl)
            step = plan.add_task(tasks)
            tasks = [step]
        else:
            plan = []
            tasks = [str(tasks)]

        seen = defaultdict(list)
        decomposed = defaultdict(list)
        result = self.seek_plan(state, plan, tasks)
        return result

    def seek_plan(self, state, pplan, tasks):
        if self.__stop_planning: return None

        LOGGER.debug("state: %s", state)
        LOGGER.debug("partial_plan: %s", pplan)
        if self.__hierarchical:
            LOGGER.debug("current partial plan: %s", list(pplan.sequential_plan()))
        else:
            LOGGER.debug("current partial plan: %s", pplan)

        while not pplan.is_empty():
            if self.__hierarchical:
                # Todo: we should pop from the open list
                #   ordered following an heuristic value.
                current_flaw = pplan.pop().operator  # pplan.get_step(pplan[0]).operator
            else:
                current_flaw = pplan.pop()

            if current_flaw not in pplan.abstract_flaws:
                # if we cannot find an operator with flaws, then the plan is good
                if self.__hierarchical:
                    LOGGER.debug("returning plan: %s", list(pplan.sequential_plan()))
                else:
                    LOGGER.debug("returning plan: %s", pplan)
                return pplan

            resolvers = current_flaw.resolve_abstract_flaw(current_flaw)
            # todo: remember to reorganise the Sets of flaws, pop current_flaw and (eventually) add its resolvers
            for r in resolvers:
                pplan.append(r)

        LOGGER.debug("nothing leads to solution")
        return None

