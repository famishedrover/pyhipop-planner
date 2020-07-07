"""Planning Problem."""
from typing import Set, Iterator, Tuple, Dict, Optional, Union, Any, Type
from collections import defaultdict
import itertools
from functools import partial
import logging
import pddl
import networkx

from ..utils.logic import Expression, Literals
from ..utils.pddl import ground_term, loop_over_predicates, iter_objects
from ..utils.poset import Poset
from ..utils.utils import negate
from .operator import GroundedAction, GroundedTask, GroundedMethod, GroundedOperator, GroundingImpossibleError

LOGGER = logging.getLogger(__name__)


class Problem:

    """Planning Problem.

    The planning problem is grounded

    :param problem: PDDL problem
    :param domain: PDDL domain
    """

    def __init__(self, problem: pddl.Problem, domain: pddl.Domain,
                 filter_static: bool = True,
                 grounding_then_tdg: bool = True,
                 htn_problem: bool = True,
                 tdg_filter_useless: bool = True):
        self.__pddl_domain = domain
        self.__pddl_problem = problem
        # Objects
        LOGGER.debug("Building types/objects mapping")
        self.__types_subtypes = Poset.subtypes_closure(domain.types)
        self.__objects_per_type = defaultdict(set)
        self.__objects = set()
        for obj in domain.constants:
            self.__objects_per_type[obj.type].add(obj.name)
            self.__objects.add(obj.name)
        for obj in problem.objects:
            self.__objects_per_type[obj.type].add(obj.name)
            self.__objects.add(obj.name)
        for t, subt in self.__types_subtypes.items():
            for st in subt:
                self.__objects_per_type[t] |= self.__objects_per_type[st]
        LOGGER.debug("%d types", len(self.__objects_per_type))
        LOGGER.debug("%d objects", len(self.__objects))
        # Predicates
        LOGGER.debug("PDDL predicates: %d", len(domain.predicates))
        self.__predicates = set()
        if filter_static:
            for action in domain.actions:
                for literal in loop_over_predicates(action.effect):
                    self.__predicates.add(literal.name)
            self.__static_predicates = set(map(lambda x: x.name, domain.predicates)) - self.__predicates
            self.__static_predicates.add('=')
            LOGGER.info("Static predicates: %d", len(self.__static_predicates))
            LOGGER.debug("Static predicates: %s", self.__static_predicates)
        else:
            self.__predicates = frozenset({pred.name for pred in domain.predicates} | {'='})
            self.__static_predicates = frozenset()
        LOGGER.info("Predicates: %d", len(self.__predicates))
        LOGGER.debug("Predicates: %s", self.__predicates)
        # Initial state
        LOGGER.debug("PDDL init literals: %d", len(problem.init))
        if filter_static:
            self.__static_literals = set(Literals.literal(lit.name,
                                                          *lit.arguments)[0]
                                         for lit in problem.init
                                         if lit.name in self.__static_predicates)
            self.__static_literals |= set(Literals.literal('=', obj, obj)
                                          for obj in self.__objects)
            LOGGER.info("Static literals: %d", len(self.__static_literals))
            LOGGER.debug("Static literals: %s", self.__static_literals)
            self.__init = frozenset(Literals.literal(lit.name, *lit.arguments)[0]
                                    for lit in problem.init
                                    if lit.name in self.__predicates)
        else:
            self.__static_literals = frozenset()
            self.__init = frozenset(Literals.literal(lit.name, *lit.arguments)[0]
                                    for lit in problem.init)
        LOGGER.info("Init literals: %d", len(self.__init))
        LOGGER.debug("Init literals: %s", self.__init)

        # Goal state
        self.__positive_goal = frozenset(ground_term(formula.name,
                                                     formula.arguments,
                                                     lambda x: x)
                                         for formula in loop_over_predicates(problem.goal, negative=False)
                                         if formula.name in self.__predicates)
        self.__negative_goal = frozenset(ground_term(formula.name,
                                                     formula.arguments,
                                                     lambda x: x)
                                         for formula in loop_over_predicates(problem.goal, positive=False)
                                         if formula.name in self.__predicates)
        LOGGER.info("Goal state: %d", len(self.__negative_goal) + len(self.__positive_goal))
        LOGGER.debug("Goal positive literals: %s", self.__positive_goal)
        LOGGER.debug("Goal negative literals: %s", self.__negative_goal)

        # Goal task
        if problem.htn:
            self.__goal_method = GroundedMethod(problem.htn, None,
                                                static_literals=self.__static_literals,
                                                static_predicates=self.__static_predicates,
                                                objects=self.__objects_per_type)
            self.__goal_task = GroundedTask(pddl.Task('__top'), None)
            self.__goal_task.add_method(self.__goal_method)

        if grounding_then_tdg:
            self.__ground_operators()
            self.__build_tdg_from_grounding()
        else:
            # Operators structures
            self.__grounding = defaultdict(list)
            self.__actions = dict()
            self.__tasks = {'__top': self.__goal_task}
            self.__methods = dict()
            # Build Task Decomposition Graph
            self.__tdg = networkx.DiGraph()
            if problem.htn:
                self.__decompose_method(self.__goal_method, parent='__top')

        if problem.htn:
            self.__tasks[str(self.__goal_task)] = self.__goal_task
            self.__methods[str(self.__goal_method)] = self.__goal_method

        LOGGER.info("Task Decomposition Graph: %d", self.__tdg.number_of_nodes())
        #networkx.drawing.nx_pydot.write_dot(self.__tdg, "problem-tdg.dot")
        LOGGER.info("Actions: %d", len(self.__actions))
        LOGGER.info("Tasks: %d", len(self.__tasks))
        LOGGER.info("Methods: %d", sum(1 for t in self.__tasks.values() for _ in t.methods))

        # Filtering nodes not accessible from root
        if htn_problem and problem.htn:
            self.__filter_tdg_htn()
            LOGGER.info("Task Decomposition Graph (HTN filter): %d", self.__tdg.number_of_nodes())
            #networkx.drawing.nx_pydot.write_dot(self.__tdg, "problem-tdg-htn.dot")
            LOGGER.info("Actions: %d", len(self.__actions))
            LOGGER.info("Tasks: %d", len(self.__tasks))
            LOGGER.info("Methods: %d", sum(1 for t in self.__tasks.values() for _ in t.methods))
        # Build Reverse DAG of SCC and remove useless nodes
        if tdg_filter_useless:
            self.__filter_tdg_scc()
            LOGGER.info("Task Decomposition Graph (useless SCC filter): %d", self.__tdg.number_of_nodes())
            #networkx.drawing.nx_pydot.write_dot(self.__tdg, "problem-tdg-useless.dot")
            LOGGER.info("Actions: %d", len(self.__actions))
            LOGGER.info("Tasks: %d", len(self.__tasks))
            LOGGER.info("Methods: %d", sum(1 for t in self.__tasks.values() for _ in t.methods))

    @property
    def name(self) -> str:
        """Problem name."""
        return self.__pddl_problem.name

    @property
    def domain(self) -> str:
        """Domain name."""
        return self.__pddl_domain.name

    @property
    def pddl(self) -> Tuple[pddl.Domain, pddl.Problem]:
        return self.__pddl_domain, self.__pddl_problem

    @property
    def literals(self):
        return self.__literals

    @property
    def init(self) -> Set[str]:
        """Get initial state."""
        return self.__init

    @property
    def goal(self) -> Tuple[Set[str], Set[str]]:
        """Get goal state. Maybe be ((), ()) if the problem is defined by a Task Network."""
        return self.__positive_goal, self.__negative_goal

    @property
    def goal_state(self):
        """Get goal state."""
        return self.__positive_goal

    @property
    def goal_task(self) -> GroundedTask:
        """Get goal task."""
        return self.__goal_task

    @property
    def actions(self) -> Iterator[GroundedAction]:
        """Returns an iterator over the actions."""
        return self.__actions.values()

    def get_action(self, action_id: str) -> GroundedAction:
        return self.__actions[action_id]

    @property
    def tasks(self) -> Iterator[GroundedTask]:
        return self.__tasks.values()

    def get_task(self, task_id: str) -> GroundedTask:
        return self.__tasks[task_id]

    def has_task(self, task_id: str) -> bool:
        return task_id in self.__tasks

    @property
    def tdg(self):
        return self.__tdg

    @property
    def types(self) -> Iterator[str]:
        """Get the set of types."""
        return self.__types_subtypes.keys()

    def subtypes(self, supertype: str) -> Set[str]:
        """Get the set of types."""
        return self.__types_subtypes[supertype]

    def objects_of(self, supertype: str) -> Set[str]:
        """Get objects of a type."""
        return self.__objects_per_type[supertype]

    def action(self, name):
        """Get an action by its name."""
        return self.__actions[name]

    def __ground_operator(self, op: Any, gop: type) -> Iterator[Type[GroundedOperator]]:
        """Ground an action."""
        variables = [itertools.product([param.name],
                                       self.objects_of(param.type))
                                       for param in op.parameters]
        for assignment in iter_objects(op.parameters, self.__objects_per_type):
            try:
                LOGGER.debug("grounding %s on variables %s", op.name, assignment)
                yield gop(op, dict(assignment),
                          static_literals=self.__static_literals,
                          static_predicates=self.__static_predicates,
                          objects=self.__objects_per_type)
            except GroundingImpossibleError as ex:
                LOGGER.debug("%s: droping operator %s!", op.name, ex.message)
                pass

    def __add_tdg_node(self, node, node_type, parent=None) -> bool:
        if node in self.__tdg.nodes:
            LOGGER.debug("Node %s already decomposed", node)
            if parent:
                self.__tdg.add_edge(parent, node)
            return False
        self.__tdg.add_node(node, node_type=node_type)
        if parent:
            self.__tdg.add_edge(parent, node)
        return True

    def __decompose_task(self, task, parent=None):
        LOGGER.debug("decomposing task %s", task)
        if not self.__add_tdg_node(str(task), 'task', parent):
            return
        self.__tasks[str(task)] = task
        for method in task.pddl.methods:
            if not (method.name in self.__grounding):
                self.__grounding[method.name] = self.__ground_operator(method, GroundedMethod)
            for gm in self.__grounding[method.name]:
                if gm.task == str(task):
                    LOGGER.debug("grounded method %s", gm)
                    self.__decompose_method(gm, parent=str(task))

    def __decompose_method(self, method, parent=None):
        LOGGER.debug("decomposing method %s", method)
        self.__tasks[str(parent)].add_method(method)
        if not self.__add_tdg_node(str(method), 'method', parent):
            return
        for subtask in method.subtasks:
            name = str(subtask)
            LOGGER.debug("decomposing subtask %s", name)
            try:
                task = self.__pddl_domain.get_task(name)
                if not (name in self.__grounding):
                    self.__grounding[name] = self.__ground_operator(task, GroundedTask)
                for gt in self.__grounding[name]:
                    self.__decompose_task(gt, parent=str(method))
            except KeyError as ex:
                action = self.__pddl_domain.get_action(name)
                if not (name in self.__grounding):
                    self.__grounding[name] = self.__ground_operator(action, GroundedAction)
                for gt in self.__grounding[name]:
                    if self.__add_tdg_node(str(gt), 'action', str(method)):
                        self.__actions[str(gt)] = gt

    def __ground_operators(self):
        # Actions
        self.__actions = {repr(ga): ga
                          for action in self.__pddl_domain.actions
                          for ga in self.__ground_operator(action, GroundedAction)}
        # Tasks
        self.__tasks = {repr(gt): gt
                        for task in self.__pddl_domain.tasks
                        for gt in self.__ground_operator(task, GroundedTask)}
        # Methods
        def in_task_or_action(op):
            return op in self.__actions or op in self.__tasks
        self.__methods = {repr(gm): gm
                          for method in self.__pddl_domain.methods
                          for gm in self.__ground_operator(method, GroundedMethod)
                          if all(map(in_task_or_action, gm.subtasks)) }
        for method in self.__methods.values():
            self.__tasks[method.task].add_method(method)

    def __build_tdg_from_grounding(self):
        # TDG
        self.__tdg = networkx.DiGraph()
        if not self.__pddl_problem.htn:
            return
        self.__tdg.add_node("__top", node_type='task')
        self.__tdg.add_node("__top_method", node_type='method')
        self.__tdg.add_edge("__top", "__top_method")
        for task in self.__tasks.keys():
            self.__tdg.add_node(task, node_type='task')
        for action in self.__actions.keys():
            self.__tdg.add_node(action, node_type='action')
        for method_name, method in self.__methods.items():
            self.__tdg.add_node(method_name, node_type='method')
            self.__tdg.add_edge(method.task, method_name)
            for subtask in method.subtasks:
                if subtask in self.__tdg.nodes:
                    self.__tdg.add_edge(method_name, subtask)
        for task in self.__goal_method.subtasks:
            self.__tdg.add_edge("__top_method", task)

    def __filter_tdg_htn(self):
        lengths = networkx.single_source_dijkstra_path_length(self.__tdg, '__top')
        self.__remove_nodes_from_fun(negate(lengths.__contains__))

    def __filter_tdg_scc(self):
        scc_graph = networkx.condensation(self.__tdg.reverse())
        scc_members = networkx.get_node_attributes(scc_graph, 'members')
        networkx.set_node_attributes(scc_graph, False, 'useless')
        networkx.set_node_attributes(self.__tdg, False, 'useless')
        scc_useless = networkx.get_node_attributes(scc_graph, 'useless')
        op_useless = networkx.get_node_attributes(self.__tdg, 'useless')
        for comp in networkx.topological_sort(scc_graph):
            pred = list(scc_graph.predecessors(comp))
            for node in scc_members[comp]:
                try:
                    op = self.__methods[node]
                    if (len(pred) == 0) and (len(op.pddl.network.subtasks) == 0):
                        pass
                    elif all(map(lambda x: scc_useless[x], pred)):
                        LOGGER.debug("Method %s is useless", op)
                        op_useless[node] = True
                except:
                    try:
                        op = self.__tasks[node]
                        if all(map(lambda x: scc_useless[x], pred)):
                            LOGGER.debug("Task %s is useless", op)
                            op_useless[node] = True
                    except:
                        pass
            scc_useless[comp] = all(map(lambda x: op_useless[x], scc_members[comp]))
        self.__remove_nodes_from_fun(op_useless.__getitem__)

    def __remove_nodes_from_fun(self, fun):
        node_types = networkx.get_node_attributes(self.__tdg, 'node_type')
        for node in list(self.__tdg.nodes):
            if fun(node):
                node_type = node_types[node]
                if node_type == 'method':
                    method = self.__methods[node]
                    try:
                        self.__tasks[method.task].remove_method(node)
                    except:
                        pass
                    del self.__methods[node]
                elif node_type == 'action':
                    del self.__actions[node]
                elif node_type == 'task':
                    del self.__tasks[node]
                self.__tdg.remove_node(node)
