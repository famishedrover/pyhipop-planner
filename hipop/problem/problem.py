"""Planning Problem."""
from typing import Set, Iterator, Tuple, Dict, Optional, Union, Any, Type, List
from collections import defaultdict
import itertools
import math
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
from ..grounding.hadd import HAdd
from ..grounding.objects import Objects

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
        self.__objects = Objects(problem=problem, domain=domain)
        # Predicates
        LOGGER.debug("PDDL predicates: %d", len(domain.predicates))
        self.__predicates = set()
        for action in domain.actions:
            for literal in loop_over_predicates(action.effect):
                self.__predicates.add(literal.name)
        self.__static_predicates = set(map(lambda x: x.name, domain.predicates)) - self.__predicates
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
            for assign in iter_objects(formula.variables, self.__objects_per_type, dict()):
                assignment = dict(assign)
                lit, _ = Literals.literal(formula.name,
                                          *[(assignment[a.name] if a.name[0] == '?' else a.name)
                                            for a in formula.variables])
                self.__static_literals.add(lit)
                if lit in init_literals: self.__static_trues.add(lit)
                else: self.__static_falses.add(lit)
        if self.__equality_requirement:
            self.__static_predicates.add('=')
            for obj in self.__objects:
                lit, _ = Literals.literal('=', obj, obj)
                self.__static_trues.add(lit)
                for o in self.__objects:
                    if obj != o:
                        lit, _ = Literals.literal('=', obj, o)
                        self.__static_falses.add(lit)
        if self.__typing_requirement:
            self.__static_predicates.add('__sortof')
            for typ, objs in self.__objects_per_type.items():
                for obj in self.__objects:
                    lit, _ = Literals.literal('__sortof', obj, typ)
                    if obj in objs:
                        self.__static_trues.add(lit)
                    else:
                        self.__static_falses.add(lit)
        LOGGER.info("Static trues: %d", len(self.__static_trues))
        LOGGER.debug("Static trues: %s", sorted(self.__static_trues))
        LOGGER.info("Static falses: %d", len(self.__static_falses))
        LOGGER.debug("Static falses: %s", sorted(self.__static_falses))
        # Dynamic Literals
        self.__literals = set()
        self.__init = set()
        self.__init_falses = set()
        for pred in self.__predicates:
            formula = domain.get_predicate(pred)
            for assign in iter_objects(formula.variables, self.__objects_per_type, dict()):
                assignment = dict(assign)
                lit, _ = Literals.literal(formula.name,
                                          *[(assignment[a.name] if a.name[0] == '?' else a.name)
                                            for a in formula.variables])
                self.__literals.add(lit)
                if lit in init_literals:
                    self.__init.add(lit)
                else:
                    self.__init_falses.add(lit)
        LOGGER.debug("All literals: %s", sorted(self.__literals))
        # Initial state
        LOGGER.info("Init literals: %d", len(self.__init))
        LOGGER.debug("Init literals: %s", sorted(self.__init))
        LOGGER.debug("Init false literals: %s", sorted(self.__init_falses))
        if LOGGER.isEnabledFor(logging.DEBUG):
            for lit in sorted(Literals.literals()):
                LOGGER.debug("-- %d %s", lit, Literals.lit_to_predicate(lit))

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

            ground = self.ground_operator
            self.__goal_methods = {repr(gm): gm
                                   for gm in ground(problem.htn, GroundedMethod, dict())}
            self.__goal_task = GroundedTask(top, None)
            for met in self.__goal_methods.values():
                self.__goal_task.add_method(met)

        # Heuristics
        ground = self.ground_operator
        self.__actions = {str(a): a for action in domain.actions
                          for a in ground(action, GroundedAction, dict())}
        self.__hadd = HAdd(self.__actions.values(), 
                           list(self.__init),
                           self.__static_trues | self.__static_falses
                           )
        LOGGER.info("Grounded actions: %d", len(self.__actions))
        LOGGER.info("Reachable actions: %d", sum(1 for a in self.__actions if not math.isinf(self.__hadd(a))))

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

        # Mutex a.k.a. Position/Motion Fluents
        mutex_list = list(self.__unique_predicates())
        LOGGER.info("Mutex predicates: %s", mutex_list)
        self.__mutex = dict()
        for pred in mutex_list:
            lits = list(l[0] for l in Literals.literals_of(pred))
            for l in lits:
                self.__mutex[l] = set(lits) - {l}
        LOGGER.debug("Mutex: %s", self.__mutex)

    @property
    def name(self) -> str:
        """Problem name."""
        return self.__pddl_problem.name

    @property
    def domain(self) -> str:
        """Domain name."""
        return self.__pddl_domain.name

    @property
    def mutex(self) -> Dict[int, Set[int]]:
        return self.__mutex

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
    def init_falses(self) -> Set[str]:
        """Get initially false literals."""
        return self.__init_falses

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
        self.__equality_requirement = True#(':equality' in requirements)
        self.__typing_requirement = True#(':typing' in requirements)
        unsupported_req = [':existential-preconditions', ':universal-effects'] 
        for req in unsupported_req:
            if req in requirements:
                LOGGER.warning("HiPOP does not support problems with %s", req)

    def __unique_predicates(self):
        def is_unique(pred, lits):
            if len(lits & self.__init) != 1:
                return False
            for op, a in self.__actions.items():
                adds, dels = a.effect
                pred_adds = adds & lits
                pred_dels = dels & lits
                pos, _ = a.support
                if len(pred_dels) == 1:
                    if len(pred_adds - pred_dels) != 1:
                        return False
                    if not (pred_dels <= pos):
                        return False
                if len(pred_adds) == 1:
                    g = pos & pred_dels
                    if len(g) != 1:
                        return False
                    if g <= pred_adds:
                        return False
            return True

        for pred in self.__predicates:
            lits = set(l[0] for l in Literals.literals_of(pred))
            if is_unique(pred, lits):
                yield pred
