import sys
import random
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
        self.OPEN = []

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
        return self.OPEN[-1]
        #return random.choice(self.OPEN)

    def get_best_flaw(self, flaws) -> int:
        """
        Returns the best flaw to resolve
        according to an heuristic.
        Actually, this heuristic is random and depends
        on the set initial ordering.
        :param flaws: the set of flaws
        :return: selected flaw
        """
        # ret = random.sample(flaws, 1)[0]
        # flaws.pop(ret)
        return flaws.pop()

    def print_plan(self, plan):
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
        if self.__hierarchical:
            LOGGER.debug("initial partial plan: %s", list(pplan.sequential_plan()))
        else:
            LOGGER.debug("initial partial plan: %s", pplan)

        # Initial partial plan
        self.OPEN = [pplan]
        seen = defaultdict(set)

        # main search loop
        while not self.empty_openlist:

            current_pplan = self.get_best_partialPlan() # OPEN.pop()
            if seen[current_pplan]:
                flaws = seen[current_pplan]
            else :
                flaws = current_pplan.abstract_flaws | current_pplan.open_links | current_pplan.threats
                seen[current_pplan] = flaws

            if not current_pplan.has_flaws:
                # if we cannot find an operator with flaws, then the plan is good
                if self.__hierarchical:
                    LOGGER.warning("returning plan: %s", list(current_pplan.sequential_plan()))
                else:
                    LOGGER.warning("returning plan: %s", current_pplan)
                return current_pplan

            LOGGER.info("Current plan has {} flaws ({} : {} : {})".format(len(flaws),
                                                                           len(current_pplan.abstract_flaws),
                                                                           len(current_pplan.open_links),
                                                                           len(current_pplan.threats) ))
            LOGGER.debug("Flaws: {}".format(flaws))

            # Todo: we should pop from a flaws list
            #   ordered following an heuristic value.
            current_flaw = self.get_best_flaw(flaws)
            LOGGER.debug("resolver candidate: %s", current_flaw)
            if not bool(flaws):
                self.OPEN.remove(current_pplan)
            resolvers = []
            if current_flaw in current_pplan.abstract_flaws:
                resolvers = current_pplan.resolve_abstract_flaw(current_flaw)  #         resolvers = list(plan.resolve_abstract_flaw(step))
            elif current_flaw in current_pplan.threats:
                resolvers = current_pplan.resolve_threat(current_flaw)
            else:
                resolvers = current_pplan.resolve_open_link(current_flaw)
            i = 0
            for r in resolvers:
                i += 1
                LOGGER.debug("new partial plan: %s", r)
                self.OPEN.append(r)
            LOGGER.debug("   just added %d plans to open lists", i)
            LOGGER.info("Open List size: %d", len(self.OPEN))
        # end while
        LOGGER.warning("nothing leads to solution")
        return None
