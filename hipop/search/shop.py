import sys
import logging
from collections import defaultdict
from copy import deepcopy, copy

from ..problem.problem import Problem
from ..problem.operator import GroundedTask
from .plan import HierarchicalPartialPlan

LOGGER = logging.getLogger(__name__)

class SHOP():

    def __init__(self, problem,
                 no_duplicate_search: bool = False,
                 hierarchical_plan: bool = False,
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

        result = self.seek_plan(state, tasks, plan, 0,
                                seen, decomposed)
        return result

    def seek_plan(self, state, tasks, branch, depth, seen, decomposed):
        if self.__stop_planning: return None

        LOGGER.debug("depth: %d", depth)
        LOGGER.debug("state: %s", state)
        LOGGER.debug("tasks: %s", tasks)
        LOGGER.debug("seen (%d): %s ", len(seen), seen)
        if self.__hierarchical:
            LOGGER.debug("current branch: %s", list(branch.sequential_plan()))
        else:
            LOGGER.debug("current branch: %s", branch)
        if not tasks:
            if self.__hierarchical:
                LOGGER.debug("returning plan: %s", list(branch.sequential_plan()))
            else:
                LOGGER.debug("returning plan: %s", branch)
            return branch
        if self.__hierarchical:
            current_task = branch.get_step(tasks[0]).operator
        else:
            current_task = tasks[0]
        LOGGER.debug("current task: %s", current_task)

        if self.problem.has_task(current_task):
            methods = self.problem.get_task(current_task).methods
            for method in methods:
                if self.__stop_planning: return None
                LOGGER.debug("depth %d : method %s", depth, method)

                if not method.is_applicable(state):
                    LOGGER.debug("method %s not applicable in state %s",
                                 method, state)
                    continue

                if state in decomposed[str(method)]:
                    LOGGER.debug("method %s already decomposed in state %s",
                                 method, state)
                    continue

                if self.__hierarchical:
                    new_branch = copy(branch)
                    substeps = list(new_branch.decompose_step(tasks[0], str(method)))
                else:
                    new_branch = branch
                    substeps = list(method.sorted_tasks)
                LOGGER.debug("# substeps: %s", substeps)

                decomposed[str(method)].append(state)

                result = self.seek_plan(state, substeps + tasks[1:],
                                        new_branch, depth+1, seen, decomposed)

                decomposed[str(method)].pop()

                if result is not None:
                    return result

                '''
                if self.__hierarchical:
                    for s in substeps:
                        branch.remove_step(s)
                '''
            LOGGER.debug("no method leads to solution")
            return None

        action = self.problem.get_action(current_task)
        LOGGER.debug("depth %d action %s", depth, action)
        if action.is_applicable(state):
            s1 = action.apply(state)
            if self.__nds and s1 in seen:
                if action in seen[s1]:
                    LOGGER.debug("couple state-action already visited {}-{}".format(s1, action))
                    return None

            seen[s1].append(action)
            if not self.__hierarchical:
                new_branch = branch + [action]
            else:
                new_branch = branch

            result = self.seek_plan(s1, tasks[1:],
                                    new_branch,
                                    depth, seen, decomposed)
            if result is None:
                seen[s1].pop()

            return result

        else:
            LOGGER.debug("action {} is NOT applicable".format(action))
            return None
