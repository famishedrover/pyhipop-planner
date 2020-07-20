import logging
from typing import Optional
from collections import defaultdict
import networkx
import math

LOGGER = logging.getLogger(__name__)
from .operator import GroundedTask, GroundedMethod, GroundedAction

class TaskDecompositionGraph:

    def __init__(self, problem, 
                 root_task: Optional[GroundedTask] = None):
        self.__graph = networkx.DiGraph()
        self.__problem = problem
        self.__heuristic = defaultdict(lambda: math.inf)

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

    def __clean(self, *nodes):
        self.__graph.remove_nodes_from(nodes)

    def __build_subtask_assignment(self, method, subtask, arguments):
        parent_assign = method.assignment
        params = subtask.parameters
        assign = dict()
        for i in range(len(arguments)):
            if arguments[i][0] != '?':
                assign[params[i].name] = arguments[i]
            else:
                assign[params[i].name] = parent_assign[arguments[i]]
        return assign

    def __build_method_assignment(self, task, method, arguments):
        parent_assign = task.assignment
        params = task.pddl.parameters
        assign = dict()
        for i in range(len(arguments)):
            param = params[i].name
            arg = arguments[i]
            if param in parent_assign:
                if arg in assign:
                    if assign[arg] != parent_assign[param]:
                        LOGGER.debug("Grounding impossible: args %s mismatch", arg)
                        raise AttributeError()
                else:
                    assign[arg] = parent_assign[param]
        return assign

    def __decompose_task(self, task: GroundedTask) -> bool:
        tname = str(task)
        if tname in self.__graph:
            LOGGER.debug("Task %s already in TDG", tname)
            return True
        LOGGER.debug("TDG decomposing task %s", tname)
        self.__graph.add_node(tname, node_type='task', op=task)
        ground = self.__problem.ground_operator
        methods = dict()
        for method in task.pddl.methods:
            try:
                ass = self.__build_method_assignment(
                    task, method, method.task.arguments)
            except AttributeError:
                continue
            for gmethod in ground(method, GroundedMethod, ass):
                if gmethod.task != tname:
                    LOGGER.error("Grounded method %s doest not match task %s",
                                 str(gmethod), tname)
                    continue
                if self.__decompose_method(gmethod):
                    methods[str(gmethod)] = gmethod
        if not methods:
            LOGGER.debug("Task %s has no valid method", tname)
            self.__clean(tname)
            return False
        self.__heuristic[tname] = min(self.__heuristic[mname]
                                      for mname in methods)
        for mname, method in methods.items():
            self.__graph.add_edge(tname, mname)
            task.add_method(method)
        return True
        
    def __decompose_method(self, method: GroundedMethod) -> bool:
        mname = str(method)
        LOGGER.debug("TDG decomposing method %s", mname)
        self.__graph.add_node(mname, node_type='method', op=method)
        domain, _ = self.__problem.pddl
        ground = self.__problem.ground_operator
        subtasks = []
        new_subtasks = []
        for (_, task_formula) in method.pddl.network.subtasks:
            if domain.has_action(task_formula.name):
                action = domain.get_action(task_formula.name)
                ass = self.__build_subtask_assignment(method, action, task_formula.arguments)
                try:
                    gaction = next(ground(action, GroundedAction, ass))
                except StopIteration:
                    LOGGER.debug("Action %s cannot be grounded", task_formula)
                    self.__clean(mname, *new_subtasks)
                    return False
                gname = str(gaction)
                if gname in self.__graph:
                    subtasks.append(gname)
                elif self.__decompose_action(gaction):
                    new_subtasks.append(gname)
                else:
                    LOGGER.debug("Method %s subtask %s cannot be decomposed",
                                 mname, gname)
                    self.__clean(mname, *new_subtasks)
                    return False

            elif domain.has_task(task_formula.name):
                task = domain.get_task(task_formula.name)
                ass = self.__build_subtask_assignment(
                    method, task, task_formula.arguments)
                try:
                    gtask = next(ground(task, GroundedTask, ass))
                except StopIteration:
                    LOGGER.debug("Task %s cannot be grounded", task)
                    self.__clean(mname, *new_subtasks)
                    return False
                gname = str(gtask)
                if gname in self.__graph:
                    subtasks.append(gname)
                elif self.__decompose_task(gtask):
                    new_subtasks.append(gname)
                else:
                    LOGGER.debug("Method %s subtask %s cannot be decomposed",
                                 mname, gname)
                    self.__clean(mname, *new_subtasks)
                    return False

            else:
                LOGGER.debug("Method %s subtask %s doest not exist!", mname, task_formula)
                self.__clean(name, *new_subtasks)
                return False

        self.__heuristic[mname] = 0
        for gtask in subtasks + new_subtasks:
            self.__graph.add_edge(mname, gtask)
            self.__heuristic[mname] += self.__heuristic[gtask]
        return True

    def __decompose_action(self, action: GroundedAction) -> bool:
        aname = str(action)
        if aname in self.__graph:
            LOGGER.debug("Action %s already in TDG", aname)
            return True
        self.__graph.add_node(aname, node_type='action', op=action)
        self.__heuristic[aname] = action.cost
        return True
