import logging
from typing import Optional
import networkx
import math

LOGGER = logging.getLogger(__name__)
from .operator import GroundedTask, GroundedMethod, GroundedAction

class TaskDecompositionGraph:

    def __init__(self, problem, 
                 root_task: Optional[GroundedTask] = None):
        self.__graph = networkx.DiGraph()
        self.__problem = problem
        self.__heuristic = dict()

        if root_task is None:
            LOGGER.error("TDG without root task is not implemented yet!")
            raise NotImplementedError()
        
        try:
            self.__decompose_task(root_task)
        except AttributeError:
            LOGGER.error("TDG is empty")

    def __len__(self):
        return self.__graph.number_of_nodes()

    def __iter__(self):
        return self.__graph.__iter__()

    @property
    def graph(self):
        return self.__graph

    def __decompose_task(self, task: GroundedTask):
        tname = str(task)
        LOGGER.debug("TDG decomposing task %s", tname)
        ground_operator = self.__problem.ground_operator
        methods = dict()
        for method in task.pddl.methods:
            for gmethod in ground_operator(method, GroundedMethod):
                if gmethod.task != tname:
                    LOGGER.debug("Grounded method %s doest not match task %s",
                                 str(gmethod), tname)
                    continue
                try:
                    self.__decompose_method(gmethod)
                    methods[str(gmethod)] = gmethod
                except AttributeError:
                    pass
        if not methods:
            LOGGER.debug("Task %s has no valid method", tname)
            raise AttributeError()
        self.__heuristic[tname] = min(self.__heuristic[mname] 
                                      for mname in methods)
        self.__graph.add_node(tname, node_type='task', op=task)
        for mname in methods:
            self.__graph.add_edge(tname, mname)
        
    def __decompose_method(self, method: GroundedMethod):
        mname = str(method)
        LOGGER.debug("TDG decomposing method %s", mname)
        domain, _ = self.__problem.pddl
        ground = self.__problem.ground_operator
        mass = method.assignment
        subtasks = dict()
        for (_, task_formula) in method.pddl.network.subtasks:
            args = task_formula.arguments
            try:
                action = domain.get_action(task_formula.name)
                params = action.parameters
                ass = {params[i].name: (mass[args[i]] if args[i].startswith('?') else args[i])
                       for i in range(len(args))}
                gaction = next(ground(action, GroundedAction, assignments=[ass]))
                self.__decompose_action(gaction)
                subtasks[str(gaction)] = gaction
            except KeyError:
                task = domain.get_task(task_formula.name)
                params = task.parameters
                ass = {params[i].name: (mass[args[i]] if args[i].startswith('?') else args[i])
                       for i in range(len(args))}
                gtask = next(ground(task, GroundedTask, assignments=[ass]))
                try:
                    self.__decompose_task(gtask)
                except AttributeError:
                    LOGGER.debug("Method %s subtask %s cannot be decomposed",
                                 mname, str(gtask))
                    raise AttributeError()
                subtasks[str(gtask)] = gtask
        self.__graph.add_node(mname, node_type='method', op=method)
        self.__heuristic[mname] = 0
        for gtask in subtasks:
            self.__graph.add_edge(mname, gtask)
            self.__heuristic[mname] += self.__heuristic[gtask]

    def __decompose_action(self, action: GroundedAction):
        aname = str(action)
        self.__heuristic[aname] = action.cost
        self.__graph.add_node(aname, node_type='action', op=action)
