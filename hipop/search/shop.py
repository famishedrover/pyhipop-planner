import sys
import logging
from collections import defaultdict

from ..problem.problem import Problem
from ..problem.operator import GroundedTask
from .plan import HierarchicalPartialPlan

LOGGER = logging.getLogger(__name__)

class SHOP():

    def __init__(self, problem, no_duplicate_search: bool = False):
        self.__problem = problem
        self.__nds = no_duplicate_search

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
        plan = HierarchicalPartialPlan(self.problem,
                                       init=False, goal_method=tasks)
        seen = defaultdict(set)
        decomposed = defaultdict(set)
        result = self.seek_plan(state, list(plan.tasks), plan, 0,
                                seen, decomposed)
        return result

    def seek_plan(self, state, tasks, branch, depth, seen, decomposed):
        LOGGER.debug("depth: %d", depth)
        LOGGER.debug("state: %s", state)
        LOGGER.debug("tasks: %s", tasks)
        LOGGER.debug("seen (%d): %s ", len(seen), seen)
        LOGGER.debug("current branch: %s", list(branch.sequential_plan()))
        if not tasks:
            LOGGER.debug("returning plan: %s", list(branch.sequential_plan()))
            return branch
        current_task = branch.get_step(tasks[0]).operator

        if isinstance(current_task, GroundedTask):
            for method in current_task.methods:
                LOGGER.debug("depth %d : method %s", depth, method)

                if not method.is_applicable(state):
                    LOGGER.debug("method %s not applicable in state %s",
                                 method, state)
                    continue

                if state in decomposed[str(method)]:
                    LOGGER.debug("method %s already decomposed in state %s",
                                 method, state)
                    continue

                substeps = list(branch.decompose_step(tasks[0], str(method)))
                LOGGER.debug("# substeps: %s", substeps)

                decomposed[str(method)].add(state)

                result = self.seek_plan(state, substeps + tasks[1:],
                                        branch, depth+1, seen, decomposed)

                decomposed[str(method)].remove(state)

                if result is not None:
                    return result

                for s in substeps:
                    branch.remove_step(s)

            LOGGER.debug("no method leads to solution")
            return None

        action = current_task #self.problem.get_action(current_task)
        LOGGER.debug("depth %d action %s", depth, action)
        if action.is_applicable(state):
            s1 = action.apply(state)
            if self.__nds and s1 in seen:
                if action in seen[s1]:
                    LOGGER.debug("couple state-action already visited {}-{}".format(s1, action))
                    return None

            seen[s1].add(action)
            #step = branch.append_action(action)
            result = self.seek_plan(s1, tasks[1:],
                                    branch,
                                    depth, seen, decomposed)
            if result is None:
                seen[s1].remove(action)
                #branch.remove_step(step)

            return result

        else:
            LOGGER.debug("action {} is NOT applicable".format(action))
            return None
