import sys
import logging
from collections import defaultdict

from ..problem.problem import Problem
from .plan import HierarchicalPartialPlan

LOGGER = logging.getLogger(__name__)

class SHOP():

    def __init__(self, problem, no_duplicate_search: bool = False):
        self.__problem = problem
        self.nds = no_duplicate_search

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
        plan = HierarchicalPartialPlan(self.problem)
        seen = {}
        decomposed = defaultdict(set)
        result = self.seek_plan(state, tasks, plan, 0, seen, decomposed)
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
        current_task = tasks[0]

        try:
            # first op is a compound task
            task = self.problem.get_task(current_task)
            for method in task.methods:
                LOGGER.debug("depth %d : method %s", depth, method)
                subtasks = list(method.sorted_tasks)
                LOGGER.debug("# subtasks: {}".format(len(subtasks)))

                if not method.is_applicable(state):
                    LOGGER.debug("method %s not applicable in state %s",
                                 method, state)
                    continue

                if state in decomposed[str(method)]:
                    LOGGER.debug("method %s already decomposed in state %s",
                                 method, state)
                    continue

                decomposed[str(method)].add(state)

                result = self.seek_plan(state, subtasks + tasks[1:],
                                        branch, depth+1, seen, decomposed)

                decomposed[str(method)].remove(state)

                if result is not None:
                    return result

            LOGGER.debug("no method leads to solution")
            return None

        except KeyError as ex:
            # primitive task, aka Action
            pass

        action = self.problem.get_action(current_task)
        LOGGER.debug("depth %d action %s", depth, action)
        if action.is_applicable(state):
            s1 = action.apply(state)
            if self.nds and seen.get(s1):
                if action in seen[s1]:
                    LOGGER.debug("couple state-action already visited {}-{}".format(s1, action))
                    return None

            if not seen.get(s1):
                seen[s1] = [action]
            else:
                seen[s1].append(action)
            step = branch.append_action(action)
            result = self.seek_plan(s1, tasks[1:],
                                    branch,
                                    depth, seen, decomposed)
            if result is None:
                seen[s1].pop()
                branch.remove_step(step)


            return result

        else:
            LOGGER.debug("action {} is NOT applicable".format(action))
            return None
