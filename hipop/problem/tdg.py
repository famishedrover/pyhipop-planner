import logging
from typing import Optional, Tuple, Set
from collections import defaultdict, namedtuple
import networkx
import math

LOGGER = logging.getLogger(__name__)
from .operator import GroundedTask, GroundedMethod, GroundedAction, GroundedOperator
from ..search.heuristics import HAdd

TDGHeuristic = namedtuple('TDGHeuristic', ['tdg', 'min_hadd', 'max_hadd'])

class TaskDecompositionGraph:

    def __init__(self, problem: 'hipop.problem.problem.Problem', 
                 root_task: Optional[GroundedTask] = None,
                 h_add: Optional[HAdd] = None):
        self.__graph = networkx.DiGraph()
        self.__problem = problem
        try:
            self.__h_add = h_add.heuristic
        except:
            self.__h_add = lambda x: 0
        self.__heuristic = defaultdict(lambda: TDGHeuristic(math.inf, math.inf, math.inf))
        if root_task is None:
            LOGGER.error("TDG without root task is not implemented yet!")
            raise NotImplementedError()
        
        self.__task_effects = defaultdict(lambda: (set(), set()))

        if not self.__decompose_task(root_task):
            LOGGER.error("TDG is empty")
        for task, h in self.__heuristic.items():
            LOGGER.debug("h_TDG(%s) = %d", task, h.tdg)

    def __len__(self):
        return self.__graph.number_of_nodes()

    def __iter__(self):
        return self.__graph.__iter__()

    @property
    def graph(self) -> networkx.DiGraph:
        return self.__graph

    def heuristic(self, node: str) -> TDGHeuristic:
        return self.__heuristic[node]

    def effect(self, node: str) -> Tuple[Set, Set]:
        return self.__task_effects[node]

    def __clean(self, *nodes: GroundedOperator):
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
        self.__heuristic[tname] = TDGHeuristic(tdg=min(self.__heuristic[mname].tdg for mname in methods),
                                               min_hadd=min(self.__heuristic[mname].min_hadd for mname in methods),
                                               max_hadd=max(self.__heuristic[mname].max_hadd for mname in methods))
        self.__graph.nodes[tname]['label'] = f"{tname} [{self.__heuristic[tname].tdg}]"

        adds, dels = set(), set()
        for mname, method in methods.items():
            self.__graph.add_edge(tname, mname)
            task.add_method(method)
            madds, mdels = self.__task_effects[mname]
            adds |= madds
            dels |= mdels
        self.__task_effects[tname] = (adds, dels)
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

                if math.isinf(self.__h_add(gname)):
                    LOGGER.debug("Action %s is not reachable", gname)
                    self.__clean(mname, *new_subtasks)
                    return False

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
                self.__clean(mname, *new_subtasks)
                return False

        h_tdg = 0
        pos, _ = method.support
        support = list(pos)# + list(neg)
        h_min = sum(self.__h_add(lit) for lit in support)
        h_max = h_min
        adds, dels = set(), set()
        for gtask in subtasks + new_subtasks:
            self.__graph.add_edge(mname, gtask)
            h_tdg += self.__heuristic[gtask].tdg
            h_min += self.__heuristic[gtask].min_hadd
            h_max += self.__heuristic[gtask].max_hadd
            tadds, tdels = self.__task_effects[gtask]
            adds |= tadds
            dels |= tdels
        self.__task_effects[mname] = (adds, dels)
        self.__heuristic[mname] = TDGHeuristic(h_tdg, h_min, h_max)
        self.__graph.nodes[mname]['label'] = f"{mname} [{h_tdg}]"
        return True

    def __decompose_action(self, action: GroundedAction) -> bool:
        aname = str(action)
        if aname in self.__graph:
            LOGGER.debug("Action %s already in TDG", aname)
            return True
        self.__graph.add_node(aname, node_type='action', op=action,
            label=f"{aname} [{action.cost}]")
        self.__heuristic[aname] = TDGHeuristic(tdg=action.cost, min_hadd=self.__h_add(aname), max_hadd=self.__h_add(aname))
        self.__task_effects[aname] = action.effect
        return True
