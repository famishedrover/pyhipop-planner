from collections import defaultdict, namedtuple
from typing import Union, Any, Iterator, Optional
import logging
import networkx

import pddl
from ..utils.poset import Poset
from ..problem.problem import Problem
from ..problem.operator import GroundedMethod, GroundedTask, GroundedAction

LOGGER = logging.getLogger(__name__)

Step = namedtuple('Step', ['operator', 'begin', 'end'])
Decomposition = namedtuple('Decomposition', ['method', 'substeps'])

class HierarchicalPartialPlan:
    def __init__(self, problem: Problem,
                 init: bool = False,
                 goal_method: Optional[GroundedMethod] = None):
        self.__problem = problem
        self.__steps = dict()
        self.__tasks = set()
        self.__poset = Poset()
        self.__hierarchy = dict()
        self.__build_init(init, goal_method)

    def __add_step(self, step: Any) -> int:
        index = len(self.__steps) + 1
        self.__steps[index] = Step(step, index, -index)
        self.__poset.add(index)
        self.__poset.add(-index)
        self.__poset.add_relation(index, -index)
        LOGGER.debug("add step %d %s", index, step)
        return index

    def __build_init(self, init, goal_method):
        _, pddl_problem = self.__problem.pddl
        if init:
            init = GroundedAction(pddl.Action('__init', effect=pddl_problem.init),
                                  None, set(), set())
            self.add_action(init)
        if goal_method is not None:
            __top = GroundedTask(pddl.Task('__top'),
                                 None, set(), set())
            __top.add_method(goal_method)
            self.add_task(__top)

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
        index = self.__add_step(action)
        return index

    def add_task(self, task: GroundedTask):
        """Add an abstract task in the plan."""
        index = self.__add_step(task)
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
        task = self.__steps[step]
        try:
            method = task.operator.get_method(method)
        except KeyError:
            LOGGER.error("Task %s has no method %s", task.operator, method)
            LOGGER.error("Task %s methods: %s", task.operator.methods)
            return False
        htn = method.task_network.poset
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
            self.__poset.add_relation(task.begin, substeps[node].begin)
            self.__poset.add_relation(substeps[node].end, task.end)
            LOGGER.debug("Adding substep %s", substeps[node])
        self.__hierarchy[step] = Decomposition(method.name,
                                               (s.begin for s in substeps.values()))
        for (u, v) in htn.edges:
            step_u = substeps[u]
            step_v = substeps[v]
            self.__poset.add_relation(step_u.end, step_v.begin)
        subgraph = self.__poset.poset.subgraph(list(s.begin for s in substeps.values())
                                               + list(s.end for s in substeps.values()))
        return [x for x in networkx.topological_sort(subgraph) if x > 0]

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
        """Return a sequential version of the primitive plan."""
        return ((i, self.__steps[i]) for i in self.__poset.topological_sort() if i > 0)
