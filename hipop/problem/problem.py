"""Planning Problem."""
from typing import Set, Iterator, Tuple, Dict, Optional, Union, Any, Type, List
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
from .tdg import TaskDecompositionGraph
from ..search.heuristics import HAdd

LOGGER = logging.getLogger(__name__)


class Problem:

    """Planning Problem.

    The planning problem is grounded

    :param problem: PDDL problem
    :param domain: PDDL domain
    """

    def __init__(self, problem: pddl.Problem, domain: pddl.Domain):
        # Problem
        self.__check_requirements(domain.requirements)
        self.__pddl_domain = domain
        self.__pddl_problem = problem
        # Objects
        self.__types_subtypes = Poset.subtypes_closure(domain.types)
        LOGGER.debug(self.__types_subtypes)
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
        LOGGER.info("Types: %d", len(self.__objects_per_type))
        LOGGER.debug("Types: %s", self.__objects_per_type.keys())
        LOGGER.info("Objects: %d", len(self.__objects))
        LOGGER.debug("Objects: %s", self.__objects)
        LOGGER.info("Objects per type: %s", self.__objects_per_type)
        # Predicates
        LOGGER.debug("PDDL predicates: %d", len(domain.predicates))
        self.__predicates = set()
        for action in domain.actions:
            for literal in loop_over_predicates(action.effect):
                self.__predicates.add(literal.name)
        self.__static_predicates = set(map(lambda x: x.name, domain.predicates)) - self.__predicates
        if self.__equality_requirement:
            self.__static_predicates.add('=')
        LOGGER.info("Static predicates: %d", len(self.__static_predicates))
        LOGGER.debug("Static predicates: %s", self.__static_predicates)
        LOGGER.info("Predicates: %d", len(self.__predicates))
        LOGGER.debug("Predicates: %s", self.__predicates)
        # Static Literals
        LOGGER.debug("PDDL init literals: %d", len(problem.init))
        init_literals = set(Literals.literal(lit.name, *lit.arguments)[0]
                            for lit in problem.init)
        self.__static_literals = set()
        self.__static_trues = set()
        self.__static_falses = set()
        for pred in self.__static_predicates:
            formula = domain.get_predicate(pred)
            LOGGER.debug("grounding static predicate %s", formula)
            for assign in iter_objects(formula.variables, self.__objects_per_type, dict()):
                assignment = dict(assign)
                lit, _ = Literals.literal(formula.name,
                                          *[(assignment[a.name] if a.name[0] == '?' else a.name)
                                            for a in formula.variables])
                self.__static_literals.add(lit)
                if lit in init_literals: self.__static_trues.add(lit)
                else: self.__static_falses.add(lit)
        for obj in self.__objects:
            lit, _ = Literals.literal('=', obj, obj)
            self.__static_trues.add(lit)
            for o in self.__objects:
                if obj != o:
                    lit, _ = Literals.literal('=', obj, o)
                    self.__static_falses.add(lit)
        LOGGER.info("Static trues: %d", len(self.__static_trues))
        LOGGER.debug("Static trues: %s", self.__static_trues)
        LOGGER.info("Static falses: %d", len(self.__static_falses))
        LOGGER.debug("Static falses: %s", self.__static_falses)
        # Initial state
        self.__init = frozenset(Literals.literal(lit.name, *lit.arguments)[0]
                                for lit in problem.init
                                if lit.name in self.__predicates)
        LOGGER.info("Init literals: %d", len(self.__init))
        LOGGER.debug("Init literals: %s", self.__init)
        for i in self.__init:
            LOGGER.debug(" %d %s",  i, Literals.lit_to_predicate(i))

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

        LOGGER.debug("PDDL actions: %d", len(domain.actions))
        LOGGER.debug("PDDL actions: %s", " ".join((a.name for a in domain.actions)))
        LOGGER.debug("PDDL methods: %d", len(domain.methods))
        LOGGER.debug("PDDL methods: %s", " ".join(
            (a.name for a in domain.methods)))
        LOGGER.debug("PDDL tasks: %d", len(domain.tasks))
        LOGGER.debug("PDDL tasks: %s", " ".join(
            (a.name for a in domain.tasks)))

        # Goal task
        if problem.htn:
            top_method = self.__pddl_problem.htn
            top = pddl.Task('__top')
            top.add_method(top_method)

            self.__goal_methods = {repr(gm): gm
                                   for gm in self.ground_operator(problem.htn, GroundedMethod, dict())}
            self.__goal_task = GroundedTask(top, None)
            for met in self.__goal_methods.values():
                self.__goal_task.add_method(met)

        # Heuristics
        self.__actions = {str(a): a for action in domain.actions
                          for a in self.ground_operator(action, GroundedAction, dict())}
        self.__hadd = HAdd(self.__actions.values(), list(
            self.__init) + list(self.__static_trues))

        # Task Decomposition Graph        
        self.__tdg = TaskDecompositionGraph(self, self.__goal_task, self.__hadd)
        LOGGER.info("Task Decomposition Graph: %d", len(self.__tdg))
        nodes = self.__tdg.graph.nodes(data=True)
        self.__actions = {n: attr['op'] for (n, attr) in nodes if attr['node_type'] == 'action'}
        self.__tasks = {n: attr['op']
                        for (n, attr) in nodes if attr['node_type'] == 'task'}
        self.__methods = {n: attr['op'] for (
            n, attr) in nodes if attr['node_type'] == 'method'}
        if LOGGER.isEnabledFor(logging.DEBUG):
            networkx.drawing.nx_pydot.write_dot(self.__tdg.graph, "problem-tdg.dot")
        LOGGER.info("Actions: %d", len(self.__actions))
        LOGGER.info("Tasks: %d", len(self.__tasks))
        LOGGER.info("Methods: %d", len(self.__methods))

    @property
    def name(self) -> str:
        """Problem name."""
        return self.__pddl_problem.name

    @property
    def domain(self) -> str:
        """Domain name."""
        return self.__pddl_domain.name

    @property
    def h_add(self) -> HAdd:
        return self.__hadd

    @property
    def pddl(self) -> Tuple[pddl.Domain, pddl.Problem]:
        return self.__pddl_domain, self.__pddl_problem

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

    def has_action(self, action_id: str) -> bool:
        return action_id in self.__actions

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

    @property
    def objects(self) -> Set[str]:
        """Get problem objects."""
        return self.__objects

    def subtypes(self, supertype: str) -> Set[str]:
        """Get the set of types."""
        return self.__types_subtypes[supertype]

    def objects_of(self, supertype: str) -> Set[str]:
        """Get objects of a type."""
        return self.__objects_per_type[supertype]

    def action(self, name):
        """Get an action by its name."""
        return self.__actions[name]

    def ground_operator(self, op: Any, gop: type, 
                        assignments: Dict[str, str]) -> Iterator[Type[GroundedOperator]]:
        """Ground an action."""
        for assignment in iter_objects(op.parameters, self.__objects_per_type, assignments):
            try:
                LOGGER.debug("grounding %s on variables %s", op.name, assignment)
                yield gop(op, dict(assignment),
                          static_trues=self.__static_trues,
                          static_falses=self.__static_falses,
                          objects=self.__objects_per_type)
            except GroundingImpossibleError as ex:
                LOGGER.debug("%s: droping operator %s!", op.name, ex.message)
                pass

    def __check_requirements(self, requirements):
        self.__equality_requirement = (':equality' in requirements)
        unsupported_req = [':existential-preconditions', ':universal-effects'] 
        for req in unsupported_req:
            if req in requirements:
                LOGGER.warning("HiPOP does not support problems with %s", req)
