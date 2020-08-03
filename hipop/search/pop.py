import sys
import random
import math
import logging
from queue import LifoQueue
from collections import defaultdict
from copy import deepcopy, copy
from sortedcontainers import SortedKeyList

from ..problem.problem import Problem
from ..problem.operator import GroundedTask
from .plan import HierarchicalPartialPlan
from ..utils.logic import Literals

LOGGER = logging.getLogger(__name__)

class POP():

    def __init__(self, problem, shoplikeSearch = False):
        self.__problem = problem
        self.__stop_planning = False
        # todo: we can initialize different OpenLists using parameters and heuristic functions
        self.OPEN = SortedKeyList(key=lambda x: x.f)
        self.OPEN_local_OL = []
        self.__shoplike = shoplikeSearch
        self.OPEN_ShoplikeLIFO = LifoQueue()



    @property
    def problem(self):
        return self.__problem

    @property
    def empty_openlist(self):
        if self.__shoplike:
            return self.OPEN_ShoplikeLIFO.empty()
        return len(self.OPEN) < 1

    @property
    def empty_local_OL_openlist(self):
        return len(self.OPEN_local_OL) < 1

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
        if self.__shoplike:
            return self.OPEN_ShoplikeLIFO.get_nowait()
        elif self.empty_local_OL_openlist:
            return self.OPEN[0]
        else:
            return self.OPEN_local_OL[0]

    """
    Here we're using an 'alternate' method to select. 
    We can boost the heuristic lowering queue.
    :returns: best partial plan, queue from where it was taken
    """
    def get_partialPlan_from_queues(self, TDG_OpenList, HADD_openList, TDG_score, HADD_score, prev_htdg, prev_hadd) -> HierarchicalPartialPlan:

        LOGGER.debug("Queues status:\n  min f(n) for htdg: {}\n htdg score: {}\n", prev_htdg, TDG_score)
        LOGGER.debug("min f(n) for hadd: {}\n hadd score: {}\n", prev_hadd, HADD_score)

        if HADD_score >= TDG_score:
            if len(HADD_openList) > 0:
                HADD_score -= 1
                selected_open = HADD_openList
            else:
                TDG_score -= 1
                selected_open = TDG_OpenList
        else:
            if len(TDG_OpenList) > 0:
                TDG_score -= 1
                selected_open = TDG_OpenList
            else:
                HADD_score -= 1
                selected_open = HADD_openList

        if len(selected_open) > 0:
            n = selected_open[0]
            return n, selected_open
        # NB: we don't have a secondary queue, but we can use the lifo from SHOP-like.

        return None, None

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

        if self.__shoplike:
            result = self.seek_plan_shoplike(None, plan)
        else:
            result = self.seek_plan(None, plan)
        # add an option
        #   return self.seek_plan_dualqueue(None, plan)
        if result:
            result.write_dot("plan.dot")
        return result

    def seek_plan_shoplike(self, state, pplan) -> HierarchicalPartialPlan:
        if self.__stop_planning: return None

        LOGGER.debug("state: %s", state)
        LOGGER.debug("partial_plan: %s", pplan)
        LOGGER.debug("initial partial plan: %s", list(pplan.sequential_plan()))

        # Initial partial plan
        self.OPEN_ShoplikeLIFO.put(pplan)
        CLOSED = list()

        # main search loop
        while not self.OPEN_ShoplikeLIFO.empty() and not self.__stop_planning:
            current_pplan = self.OPEN_ShoplikeLIFO.get_nowait()

            if LOGGER.isEnabledFor(logging.DEBUG):
                current_pplan.write_dot(f"current-plan.dot")
            LOGGER.debug("current plan id: %s (cost function: %s)", id(current_pplan), current_pplan.f)

            if not current_pplan.has_flaws:
                # if we cannot find an operator with flaws, then the plan is good
                LOGGER.warning("returning plan: %s", list(current_pplan.sequential_plan()))
                return current_pplan

            if current_pplan in CLOSED:
                LOGGER.debug(
                    "current plan %d in CLOSED: skipping plan", id(current_pplan))
                continue            

            if not current_pplan.compute_flaw_resolvers():
                CLOSED.append(current_pplan)
                LOGGER.debug(
                    "current plan %d has no resolver: closing plan", id(current_pplan))
                continue

            LOGGER.info("Current plan has {} flaws ({}/{} : {}/{} : {}/{})".format(len(current_pplan.pending_abstract_flaws) + len(current_pplan.pending_open_links) + len(current_pplan.pending_threats),
                                                                                   len(current_pplan.pending_abstract_flaws), len(
                                                                                       current_pplan.abstract_flaws),
                                                                                   len(current_pplan.pending_open_links), len(
                                                                                       current_pplan.open_links),
                                                                                   len(current_pplan.pending_threats), len(current_pplan.threats)))
            
            successors = list()
            while current_pplan.has_pending_flaws:
                if len(current_pplan.pending_threats) > 0:
                    current_flaw, _ = current_pplan.pending_threats.pop(0)
                elif len(current_pplan.pending_open_links) > 0:
                    if len(current_pplan.pending_abstract_flaws) > 0:
                        best_ol, _ = current_pplan.pending_open_links[0]
                        best_abstract = current_pplan.pending_abstract_flaws[0]
                        if current_pplan.poset.is_less_than(best_ol.step, best_abstract):
                            current_flaw, _ = current_pplan.pending_open_links.pop(0)
                        else:
                            current_flaw = current_pplan.pending_abstract_flaws.pop(0)
                    else:
                        current_flaw, _ = current_pplan.pending_open_links.pop(0)

                else:
                    current_flaw = current_pplan.pending_abstract_flaws.pop(0)

                LOGGER.debug("current flaw: %s", current_flaw)

                resolvers = current_pplan.resolvers(current_flaw)
                for r in resolvers:
                    #LOGGER.debug("resolver: %s", id(r))
                    if r in CLOSED:
                        LOGGER.debug("plan %s already in CLOSED set", id(r))
                    else:
                        successors.append(r)

            LOGGER.debug("   just added %d plans to open lists", len(successors))

            LOGGER.debug("closing current plan")
            CLOSED.append(current_pplan)

            successors.reverse()
            for plan in successors:
                self.OPEN_ShoplikeLIFO.put(plan)

            LOGGER.info("Open List size: %d", self.OPEN_ShoplikeLIFO.qsize())
            LOGGER.info("Closed List size: %d", len(CLOSED))
        # end while
        LOGGER.warning("nothing leads to solution")
        return None


    """
    Implements a dual-queue best first search
    """
    def seek_plan_dualqueue(self, state, pplan) -> HierarchicalPartialPlan:

        OPEN_Tdg = SortedKeyList(key=lambda x: x.htdg)
        OPEN_Hadd = SortedKeyList(key=lambda x: x.f)
        TDG_score = HADD_score = 1
        prev_htdg = prev_hadd = math.inf

        if self.__stop_planning: return None

        LOGGER.debug("state: %s", state)
        LOGGER.debug("partial_plan: %s", pplan)
        LOGGER.debug("initial partial plan: %s", list(pplan.sequential_plan()))

        # Initial partial plan
        OPEN_Tdg.add(pplan)
        OPEN_Hadd.add(pplan)
        CLOSED = list()

        # main search loop
        while (OPEN_Hadd or OPEN_Tdg) and not self.__stop_planning:

            current_pplan, current_OPEN = self.get_partialPlan_from_queues(OPEN_Tdg, OPEN_Hadd, TDG_score, HADD_score, prev_htdg, prev_hadd)
            if LOGGER.isEnabledFor(logging.DEBUG):
                current_pplan.write_dot(f"current-plan.dot")
            LOGGER.debug("current plan id: %s (cost function: %s)", id(current_pplan), current_pplan.f)

            if not current_pplan.has_flaws:
                # if we cannot find an operator with flaws, then the plan is good
                LOGGER.warning("returning plan: %s", list(current_pplan.sequential_plan()))
                return current_pplan

            if current_pplan in CLOSED:
                OPEN_Hadd.remove(current_pplan)
                OPEN_Tdg.remove(current_pplan)
                LOGGER.debug(
                    "current plan %d in CLOSED: removing plan", id(current_pplan))
                continue
            if not current_pplan.compute_flaw_resolvers():
                OPEN_Hadd.remove(current_pplan)
                OPEN_Tdg.remove(current_pplan)
                CLOSED.append(current_pplan)
                LOGGER.debug(
                    "current plan %d has no resolver: closing plan", id(current_pplan))
                continue

            if current_pplan.hadd < prev_hadd :
                prev_hadd = current_pplan.hadd
                HADD_score += 10
            if current_pplan.htdg < prev_htdg:
                prev_htdg = current_pplan.htdg
                TDG_score += 10

            LOGGER.info("Current plan has {} flaws ({} : {} : {})".format(len(current_pplan.pending_abstract_flaws) + len(current_pplan.pending_open_links) + len(current_pplan.pending_threats),
                                                                           len(current_pplan.pending_abstract_flaws),
                                                                           len(current_pplan.pending_open_links),
                                                                           len(current_pplan.pending_threats) ))
            #while self.__shoplike and current_pplan.has_pending_flaws:

            current_flaw = current_pplan.get_best_flaw()
            LOGGER.debug("resolver candidate: %s", current_flaw)
            # If it's open link, try tto solve all it resolvers.

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
                    OPEN_Hadd.add(r)
                    OPEN_Tdg.add(r)
            LOGGER.debug("   just added %d plans to open lists", i)

            if close_plan:
                LOGGER.debug("closing current plan")
                CLOSED.append(current_pplan)
                try:
                    OPEN_Hadd.remove(current_pplan)
                except ValueError:
                    pass
                try:
                    OPEN_Tdg.remove(current_pplan)
                except ValueError:
                    pass
            LOGGER.info("Open List size: %d", len(self.OPEN))
            LOGGER.info("Closed List size: %d", len(CLOSED))
        # end while
        LOGGER.warning("nothing leads to solution")
        return None


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

            if current_pplan in CLOSED:
                self.OPEN.remove(current_pplan)
                LOGGER.debug(
                    "current plan %d in CLOSED: removing plan", id(current_pplan))
                if not self.empty_local_OL_openlist:
                    self.OPEN_local_OL.remove(current_pplan)
                continue
            if not current_pplan.compute_flaw_resolvers():
                self.OPEN.remove(current_pplan)
                CLOSED.append(current_pplan)
                LOGGER.debug(
                    "current plan %d has no resolver: closing plan", id(current_pplan))
                if not self.empty_local_OL_openlist:
                    self.OPEN_local_OL.remove(current_pplan)
                continue

            LOGGER.info("Current plan has {} flaws ({} : {} : {})".format(len(current_pplan.pending_abstract_flaws) + len(current_pplan.pending_open_links) + len(current_pplan.pending_threats),
                                                                           len(current_pplan.pending_abstract_flaws),
                                                                           len(current_pplan.pending_open_links),
                                                                           len(current_pplan.pending_threats) ))
            #while self.__shoplike and current_pplan.has_pending_flaws:

            current_flaw = current_pplan.get_best_flaw()
            LOGGER.debug("resolver candidate: %s", current_flaw)
            # If it's open link, try tto solve all it resolvers.

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
                    if current_flaw in current_pplan.open_links:
                        self.OPEN_local_OL.append(r)
                    self.OPEN.add(r)
            LOGGER.debug("   just added %d plans to open lists", i)

            if close_plan:
                LOGGER.debug("closing current plan")
                CLOSED.append(current_pplan)
                self.OPEN.remove(current_pplan)
                if not self.empty_local_OL_openlist:
                    try: # in case it's the fist plan
                        self.OPEN_local_OL.remove(current_pplan)
                    except ValueError:
                        pass
            LOGGER.info("Open List size: %d", len(self.OPEN))
            LOGGER.info("Closed List size: %d", len(CLOSED))
        # end while
        LOGGER.warning("nothing leads to solution")
        return None
