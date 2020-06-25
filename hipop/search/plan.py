from collections import defaultdict
from typing import Union, Any, Iterator
import logging

from ..utils.poset import Poset
from ..problem.problem import Problem
from ..problem.operator import GroundedMethod, GroundedTask, GroundedAction

LOGGER = logging.getLogger(__name__)

class HierarchicalPartialPlan:
    def __init__(self, problem: Problem):
        self.__problem = problem
        self.__steps = dict()
        self.__tasks = set()
        self.__poset = Poset()
        self.__hierarchy = defaultdict(lambda: ("", set()))

    def __add_step(self, step: Any) -> int:
        index = len(self.__steps)
        self.__steps[index] = step
        LOGGER.debug("add step %d %s", index, step)
        return index

    @property
    def tasks(self):
        return self.__tasks

    def get_decomposition(self, task: int):
        return self.__hierarchy[task]

    def get_step(self, step: int) -> Any:
        """Get step from index."""
        return self.__steps[step]

    def append_action(self, action: GroundedAction) -> int:
        """Append an action at the end of the plan."""
        # Add step
        index = self.__add_step(action)
        # Add to poset
        max_elements = self.__poset.maximal_elements()
        self.__poset.add(index)
        for step in max_elements:
            LOGGER.debug("add relation from %d to %d", step, index)
            self.__poset.add_relation(step, index)
        return index

    def add_task(self, task: GroundedTask):
        """Add an abstract task in the plan."""
        index = self.__add_step(task)
        self.__tasks.add(index)
        return index

    def hierarchy(self, task, method, subtasks):
        """Define the hierarchical relation of tasks / subtasks."""
        if all(map(lambda x: x in self.__steps, [task]+subtasks)):
            self.__hierarchy.update({task: (str(method), subtasks)})
            LOGGER.debug("Task %s decomposes using %s into %s", task, method, subtasks)
            return True
        else:
            LOGGER.error("Some task is not defined in the plan")
            return False

    @property
    def hierarchy_flaws(self) -> Iterator[int]:
        """Return the set of Hierarchy Flaws in the plan."""
        return (x for x in self.__tasks if x not in self.__hierarchy)

    def graphviz_string(self) -> str:
        self.__poset.reduce()
        graph = 'digraph {\n' + '\n'.join(map(lambda x: f"{x[0]} -> {x[1]};", self.__poset._edges))
        for task, method in self.__hierarchy.items():
            graph += f'\nsubgraph cluster_{task} ' + "{\nstyle = dashed;\n"
            graph += f'\nlabel = "{task} / {method[0]}";\n'
            graph += "\n".join(map(lambda x: f"{x};", method[1]))
            graph += "}\n"
        graph += "}"
        return graph

    def sequential_plan(self):
        """Return a sequential version of the primitive plan."""
        return ((i, self.__steps[i]) for i in self.__poset.topological_sort())
