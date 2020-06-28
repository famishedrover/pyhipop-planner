"""Planning Problem."""
from typing import Set, Iterator, Tuple, Dict, Optional, Union, Any, Type
from collections import defaultdict
import itertools
from functools import partial
import logging
import pddl
import networkx

from ..utils.pddl import ground_term, loop_over_predicates
from ..utils.poset import Poset
from .operator import GroundedAction, GroundedTask, GroundedMethod, GroundedOperator, GroundingImpossibleError

LOGGER = logging.getLogger(__name__)


class Problem:

    """Planning Problem.

    The planning problem is grounded

    :param problem: PDDL problem
    :param domain: PDDL domain
    """

    def __init__(self, problem: pddl.Problem, domain: pddl.Domain):
        self.__pddl_domain = domain
        self.__pddl_problem = problem
        # Objects
        LOGGER.debug("Building types/objects mapping")
        self.__types_subtypes = Poset.subtypes_closure(domain.types)
        self.__objects_per_type = defaultdict(set)
        for obj in domain.constants:
            self.__objects_per_type[obj.type].add(obj.name)
        for obj in problem.objects:
            self.__objects_per_type[obj.type].add(obj.name)
        LOGGER.debug("%d types", len(self.__objects_per_type))
        LOGGER.debug("%d objects", len(list(domain.constants) + problem.objects))
        # Predicates
        LOGGER.debug("PDDL predicates: %d", len(domain.predicates))
        self.__predicates = set()
        for action in domain.actions:
            for literal in loop_over_predicates(action.effect):
                self.__predicates.add(literal.name)
        LOGGER.info("Predicates: %d", len(self.__predicates))
        LOGGER.debug("Predicates: %s", self.__predicates)
        self.__static_predicates = set(map(lambda x: x.name, domain.predicates)) - self.__predicates
        LOGGER.info("Static predicates: %d", len(self.__static_predicates))
        LOGGER.debug("Static predicates: %s", self.__static_predicates)
        # Initial state
        self.__static_literals = frozenset(ground_term(lit.name, lit.arguments)
                                           for lit in problem.init
                                           if lit.name in self.__static_predicates)
        LOGGER.debug("PDDL init literals: %d", len(problem.init))
        LOGGER.info("Static literals: %d", len(self.__static_literals))
        LOGGER.debug("Static literals: %s", self.__static_literals)
        self.__init = frozenset(ground_term(lit.name, lit.arguments)
                                for lit in problem.init
                                if lit.name in self.__predicates)
        LOGGER.info("Init literals: %d", len(self.__init))
        LOGGER.debug("Init literals: %s", self.__init)
        # Literals
        self.__literals = list()
        for predicate in filter(lambda x: x.name in self.__predicates, domain.predicates):
            variables = [itertools.product([param.name],
                                           self.objects_of(param.type))
                         for param in predicate.variables]
            def assign(a):
                return ground_term(predicate.name,
                                   map(lambda x: x.name, predicate.variables),
                                   dict(a).__getitem__)
            self.__literals += map(assign, itertools.product(*variables))
        LOGGER.info("Literals: %d", len(self.__literals))
        LOGGER.debug("Literals: %s", self.__literals)
        self.__literals_int_str = {i: self.__literals[i] for i in range(len(self.__literals))}
        self.__literals_str_int = {v: k for k, v in self.__literals_int_str.items()}
        # Actions
        self.__actions = {repr(ga): ga
                          for action in domain.actions
                          for ga in self.__ground_operator(action, GroundedAction)}
        LOGGER.info("Actions: %d", len(self.__actions))
        # Tasks
        self.__tasks = {repr(gt): gt
                        for task in domain.tasks
                        for gt in self.__ground_operator(task, GroundedTask)}
        LOGGER.info("Tasks: %d", len(self.__tasks))
        # Methods
        self.__methods = {repr(gm): gm
                          for method in domain.methods
                          for gm in self.__ground_operator(method, GroundedMethod)}
        for method in self.__methods.values():
            self.__tasks[method.task].add_method(method)
        LOGGER.info("Methods: %d", sum(1 for t in self.__tasks.values() for _ in t.methods))

        # Goal state
        self.__positive_goal = frozenset(ground_term(formula.name,
                                                     formula.arguments,
                                                     lambda x: x)
                                         for formula in loop_over_predicates(problem.goal, negative=False))
        self.__negative_goal = frozenset(ground_term(formula.name,
                                                     formula.arguments,
                                                     lambda x: x)
                                         for formula in loop_over_predicates(problem.goal, positive=False))

        # Goal task
        self.__goal_task = GroundedMethod(problem.htn, None, self.__predicates, self.__static_literals) if problem.htn else None
        pp_task = lambda t: f"[{t}]{self.__goal_task.subtask(t)}"

        # Build Task Decomposition Graph
        if problem.htn:
            self.__tdg = networkx.DiGraph()
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
                    self.__tdg.add_edge(method_name, subtask)
            for task in self.__goal_task.subtasks:
                self.__tdg.add_edge("__top_method", task)
        else:
            self.__tdg = None

    @property
    def tdg(self):
        return self.__tdg

    @property
    def name(self) -> str:
        """Problem name."""
        return self.__pddl_problem.name

    @property
    def domain(self) -> str:
        """Domain name."""
        return self.__pddl_domain.name

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
    def goal_task(self) -> GroundedMethod:
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

    @property
    def types(self) -> Iterator[str]:
        """Get the set of types."""
        return self.__types_subtypes.keys()

    def subtypes(self, supertype: str) -> Set[str]:
        """Get the set of types."""
        return self.__types_subtypes[supertype]

    def objects_of(self, supertype: str) -> Set[str]:
        """Get objects of a type."""
        return (set(obj
                    for subtype in self.subtypes(supertype)
                    for obj in self.__objects_per_type[subtype])
                | self.__objects_per_type[supertype])

    def action(self, name):
        """Get an action by its name."""
        return self.__actions[name]

    def __ground_operator(self, op: Any, gop: type) -> Iterator[Type[GroundedOperator]]:
        """Ground an action."""
        variables = [itertools.product([param.name],
                                       self.objects_of(param.type))
                     for param in op.parameters]
        for assignment in itertools.product(*variables):
            try:
                yield gop(op, dict(assignment), self.__predicates, self.__static_literals)
            except GroundingImpossibleError as ex:
                #LOGGER.debug("%s: droping operator!", ex.message)
                pass
