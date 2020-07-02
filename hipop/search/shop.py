import sys
import logging
from collections import defaultdict

from ..problem.problem import Problem
from ..problem.operator import GroundedTask
from .plan import HierarchicalPartialPlan

LOGGER = logging.getLogger(__name__)

class SHOP():

    def __init__(self, problem,
                 no_duplicate_search: bool = False,
                 hierarchical_plan: bool = False):
        self.__problem = problem
        self.__nds = no_duplicate_search
        self.__hierarchical = hierarchical_plan

    @property
    def problem(self):
        return self.__problem

    def find_plan(self, state, tasks):
        """
         Searches for a plan that accomplishes tasks in state.
         Basically performs a DFS.
        :param state: Initial state of the search
        :return: the plan
        """
        if self.__hierarchical:
            plan = HierarchicalPartialPlan(self.problem,
                                           init=False, goal_method=tasks)
            tasks = list(plan.tasks)
        else:
            plan = []
            tasks = list(tasks.sorted_tasks)

        seen = defaultdict(list)
        decomposed = defaultdict(list)
        result = self.seek_plan(state, tasks, plan, 0,
                                seen, decomposed)
        return result

    def seek_plan(self, state, tasks, branch, depth, seen, decomposed):
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

        if ((self.__hierarchical and isinstance(current_task, GroundedTask))
            or self.problem.has_task(current_task)):
            if self.__hierarchical:
                methods = current_task.methods
            else:
                methods = self.problem.get_task(current_task).methods
            for method in methods:
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
                    substeps = list(branch.decompose_step(tasks[0], str(method)))
                else:
                    substeps = list(method.sorted_tasks)
                LOGGER.debug("# substeps: %s", substeps)

                decomposed[str(method)].append(state)

                result = self.seek_plan(state, substeps + tasks[1:],
                                        branch, depth+1, seen, decomposed)

                decomposed[str(method)].pop()

                if result is not None:
                    return result

                if self.__hierarchical:
                    for s in substeps:
                        branch.remove_step(s)

            LOGGER.debug("no method leads to solution")
            return None

        if self.__hierarchical:
            action = current_task
        else:
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
