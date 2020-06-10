"""Planning Problem."""

from typing import Set, Iterator, Tuple, List, Dict
import logging
logger = logging.getLogger(__name__)

from collections import defaultdict
import itertools
import pddl

from .utils.pddl import pythonize, ground_formula, ground_term
from .utils.graph import subtypes_closure

class Effect:
    def __init__(self, positive_condition=frozenset(),
                 negative_condition=frozenset(),
                 add_literals=frozenset(), del_literals=frozenset()):
        self.__pos = positive_condition
        self.__neg = negative_condition
        self.__add = add_literals
        self.__del = del_literals

    def applicable(self, state: Set[str]) -> Tuple[Set[str], Set[str]]:
        """Returns applicable effects."""
        if (self.__pos <= state and self.__neg.isdisjoint(state)):
            return self.__add, self.__del
        return frozenset(), frozenset()

class GroundedAction:

    """Planning Action.

    :param action: input PDDL action
    :param assignment: action arguments as a dict of variable -> object
    """

    def __init__(self,
                 action: pddl.Action,
                 assignment: Dict[str, str]):
        self.__name = action.name
        self.__assignment = assignment

        self.__repr = ground_term(self.name,
                                  map(lambda x: x.name,
                                      action.parameters),
                                  assignment.__getitem__)

        self.__positive_pre = set()
        self.__negative_pre = set()
        self.__effects = set()

        ground_formula(action.precondition,
                       self.__assignment,
                       self.__positive_pre,
                       self.__negative_pre)

        addlit, dellit = set(), set()
        ground_formula(action.effect, self.__assignment,
                       addlit, dellit, self.__effects)
        self.__effects.add(Effect(frozenset(), frozenset(), addlit, dellit))

    def __repr__(self):
        return self.__repr

    @property
    def name(self) -> str:
        """Get action name."""
        return self.__name

    @property
    def preconditions(self) -> Tuple[Set[str], Set[str]]:
        """Get preconditions as a pair of (positive pre, negative pre)."""
        return (self.__positive_pre, self.__negative_pre)

    @property
    def effects(self) -> Set[Effect]:
        """Get set of effects."""
        return self.__effects

    def is_applicable(self, state: Set[str]) -> bool:
        """Test if action is applicable in state."""
        logger.debug(f"is {repr(self)} applicable in {state}?")
        return (self.__positive_pre <= state
                and
                self.__negative_pre.isdisjoint(state)
                )

    def apply(self, state: Set[str]) -> Set[str]:
        """Apply action to state and return a new state."""
        logger.debug(f"apply {repr(self)} to {state}:")
        positive = set()
        negative = set()
        for eff in self.effects:
            pos, neg = eff.applicable(state)
            positive |= pos
            negative |= neg
        logger.debug(f"literals to add: {positive}")
        logger.debug(f"literals to del: {negative}")
        new_state = (state | positive) - negative
        logger.debug(f"result in {new_state}")
        return new_state

class Problem:

    """Planning Problem.

    The planning problem is grounded

    :param problem: PDDL problem
    :param domain: PDDL domain
    """

    def __init__(self, problem: pddl.Problem, domain: pddl.Domain):
        self.__name = problem.name
        self.__domain = domain.name

        self.__types_subtypes = subtypes_closure(domain.types)
        self.__objects_per_type = defaultdict(set)
        for obj in domain.constants:
            self.__objects_per_type[obj.type].add(obj.name)
        for obj in problem.objects:
            self.__objects_per_type[obj.type].add(obj.name)

        self.__actions = {}
        for action in domain.actions:
            for ga in self.__ground_action(action):
                logger.info(f"grounded action {ga}")
                self.__actions[repr(ga)] = ga

        self.__init = frozenset(ground_term(lit.name, lit.arguments)
                                for lit in problem.init)
        logger.info(f"initial state: {self.__init}")

    @property
    def name(self) -> str:
        """Problem name."""
        return self.__name

    @property
    def domain(self) -> str:
        """Domain name."""
        return self.__domain

    @property
    def init(self) -> Set[str]:
        """Get initial state."""
        return self.__init

    @property
    def types(self) -> Iterator[str]:
        """Get the set of types."""
        return self.__types_subtypes.keys()

    def subtypes(self, supertype: str) -> Set[str]:
        """Get the set of types."""
        return self.__types_subtypes[supertype]

    def objects_of(self, supertype: str) -> Set[str]:
        """Get objects of a type."""
        return set(obj
                   for subtype in self.subtypes(supertype)
                   for obj in self.__objects_per_type[subtype]) | self.__objects_per_type[supertype]

    def action(self, name):
        return self.__actions[name]

    def __ground_action(self, action: pddl.Action) -> Iterator[GroundedAction]:
        """Ground an action."""
        variables = []
        for param in action.parameters:
            variables.append([(param.name, obj)
                              for obj in self.objects_of(param.type)])
        for assignment in itertools.product(*variables):
            yield GroundedAction(action, dict(assignment))
