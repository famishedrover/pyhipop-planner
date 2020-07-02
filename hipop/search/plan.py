from collections import defaultdict, namedtuple
from typing import Union, Any, Iterator, Optional
from copy import deepcopy, copy
import logging
import networkx

import pddl
from ..utils.poset import Poset, IncrementalPoset
from ..problem.problem import Problem
from ..problem.operator import GroundedMethod, GroundedTask, GroundedAction

LOGGER = logging.getLogger(__name__)

Step = namedtuple('Step', ['operator', 'begin', 'end'])
Decomposition = namedtuple('Decomposition', ['method', 'substeps'])

class HierarchicalPartialPlan:
    def __init__(self, problem: Problem,
                 init: bool = False,
                 poset_inc_impl: bool = True):
        self.__problem = problem
        self.__steps = dict()
        self.__tasks = set()
        self.__poset = (IncrementalPoset() if poset_inc_impl else Poset())
        self.__hierarchy = dict()
        if init:
            self.__build_init()

    def __add_step(self, step: str) -> int:
        index = len(self.__steps) + 1
        self.__steps[index] = Step(step, index, -index)
        self.__poset.add(index)
        self.__poset.add(-index)
        self.__poset.add_relation(index, -index)
        LOGGER.debug("add step %d %s", index, step)
        return index

    def __build_init(self):
        _, pddl_problem = self.__problem.pddl
        init = GroundedAction(pddl.Action('__init', effect=pddl_problem.init),
                              None, set(), set())
        self.add_action(init)

    def __copy__(self):
        new_plan = HierarchicalPartialPlan(self.__problem, False)
        new_plan.__steps = copy(self.__steps)
        new_plan.__tasks = copy(self.__tasks)
        new_plan.__hierarchy = copy(self.__hierarchy)
        new_plan.__poset = deepcopy(self.__poset)
        return new_plan

    @property
    def tasks(self):
        return self.__tasks

    def get_decomposition(self, task: int):
        return self.__hierarchy[task]

    def get_step(self, step: int) -> Any:
        """Get step from index."""
        return self.__steps[step]

    def remove_step(self, index: int):
        LOGGER.debug("removing step %d", index)
        step = self.__steps[index]
        self.__poset.remove(step.begin)
        self.__poset.remove(step.end)
        if index in self.__tasks:
            self.__tasks.remove(index)
        if index in self.__hierarchy:
            for substep in self.__hierarchy[index].substeps:
                try:
                    self.remove_step(substep)
                except KeyError:
                    pass
            del self.__hierarchy[index]
        del self.__steps[index]

    def add_action(self, action: GroundedAction):
        """Add an action in the plan."""
        index = self.__add_step(str(action))
        return index

    def add_task(self, task: GroundedTask):
        """Add an abstract task in the plan."""
        index = self.__add_step(str(task))
        self.__tasks.add(index)
        return index

    def decompose_step(self, step: int, method: str) -> bool:
        """Decompose a hierarchical task already in the plan."""
        if step not in self.__steps:
            LOGGER.error("Step %d is not in the plan", step)
            return False
        if step not in self.__tasks:
            LOGGER.error("Step %d is not a task in the plan", step)
            return False
        task_step = self.__steps[step]
        task = self.__problem.get_task(task_step.operator)
        try:
            method = task.get_method(method)
        except KeyError:
            LOGGER.error("Task %s has no method %s", task, method)
            LOGGER.error("Task %s methods: %s", task.methods)
            return False
        htn = method.task_network
        substeps = dict()
        for node in htn.nodes:
            subtask_name = method.subtask(node)
            try:
                subtask = self.__problem.get_task(subtask_name)
                substeps[node] = self.__steps[self.add_task(subtask)]
                LOGGER.debug("Add step %d %s", substeps[node].begin, subtask)
            except:
                subtask = self.__problem.get_action(subtask_name)
                substeps[node] = self.__steps[self.add_action(subtask)]
            self.__poset.add_relation(task_step.begin, substeps[node].begin)
            self.__poset.add_relation(substeps[node].end, task_step.end)
            LOGGER.debug("Adding substep %s", substeps[node])
        self.__hierarchy[step] = Decomposition(method.name,
                                               frozenset(s.begin for s in substeps.values()))
        for (u, v) in htn.edges:
            step_u = substeps[u]
            step_v = substeps[v]
            self.__poset.add_relation(step_u.end, step_v.begin)
        subgraph = list(s.begin for s in substeps.values()) + list(s.end for s in substeps.values())
        return filter(lambda x: x > 0, self.__poset.topological_sort(subgraph))

    @property
    def hierarchy_flaws(self) -> Iterator[int]:
        """Return the set of Hierarchy Flaws in the plan."""
        return (x for x in self.__tasks if x not in self.__hierarchy)

    def graphviz_string(self) -> str:
        self.__poset.reduce()
        graph = 'digraph {\n' + '\n'.join(map(lambda x: f"{x[0]} -> {x[1]};", self.__poset._edges))
        for index, step in self.__steps.items():
            if index in self.__tasks and index in self.__hierarchy:
                decomp = self.__hierarchy[index]
                graph += f'\nsubgraph cluster_{index} ' + "{\nstyle = dashed;\n"
                graph += f'label = "{index} / {step.operator} / {decomp.method}";\n'
                graph += "".join(map(lambda x: f"cluster_{x};\n", decomp.substeps))
                graph += f"{step.begin}; {step.end};\n"
                graph += "}\n"
            else:
                graph += f'\nsubgraph cluster_{index} ' + "{\nstyle = solid;\n"
                graph += f'label = "{index} / {step.operator}";\n'
                graph += f"{step.begin}; {step.end};\n"
                graph += "}\n"
        graph += "}"
        return graph

    def sequential_plan(self):
        """Return a sequential version of the plan."""
        return ((i, self.__steps[i]) for i in self.__poset.topological_sort() if i > 0)
