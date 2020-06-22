"""Planning Problem."""
from typing import Set, Iterator, Tuple, Dict, Optional, Union, Any, Type
from collections import defaultdict
import itertools
from functools import partial
import logging
import pddl

from ..utils.pddl import ground_formula, ground_term
from ..utils.poset import Poset
from .effect import Effect
from .operator import GroundedAction, GroundedTask, GroundedMethod, GroundedOperator

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
        self.__types_subtypes = Poset.subtypes_closure(domain.types)
        self.__objects_per_type = defaultdict(set)
        for obj in domain.constants:
            self.__objects_per_type[obj.type].add(obj.name)
        for obj in problem.objects:
            self.__objects_per_type[obj.type].add(obj.name)
        # Literals
        self.__literals = list()
        for predicate in domain.predicates:
            variables = [itertools.product([param.name],
                                           self.objects_of(param.type))
                         for param in predicate.variables]
            def assign(a):
                return ground_term(predicate.name,
                                   map(lambda x: x.name, predicate.variables),
                                   dict(a).__getitem__)
            self.__literals += map(assign, itertools.product(*variables))
        self.__literals_int_str = {i: self.__literals[i] for i in range(len(self.__literals))}
        self.__literals_str_int = {v: k for k, v in self.__literals_int_str.items()}
        # Actions
        self.__actions = {repr(ga): ga
                          for action in domain.actions
                          for ga in self.__ground_operator(action, GroundedAction)}
        # Tasks
        self.__tasks = {repr(gt): gt
                        for task in domain.tasks
                        for gt in self.__ground_operator(task, GroundedTask)}
        # Methods
        self.__methods = {repr(gm): gm
                          for method in domain.methods
                          for gm in self.__ground_operator(method, GroundedMethod)}
        for method in self.__methods.values():
            self.__tasks[method.task].add_method(method)

        # Initial state
        self.__init = frozenset(ground_term(lit.name, lit.arguments)
                                for lit in problem.init)
        # Goal state
        self.__positive_goal = set()
        self.__negative_goal = set()
        ground_formula(problem.goal, lambda x: x,
                       self.__positive_goal, self.__negative_goal)
        self.__goal = frozenset(self.__positive_goal)
        # Goal task
        self.__goal_task = GroundedMethod(problem.htn) if problem.htn else None
        pp_task = lambda t: f"[{t}]{self.__goal_task.subtask(t)}"

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
        return self.__goal

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
            yield gop(op, dict(assignment))
