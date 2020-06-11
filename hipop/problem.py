"""Planning Problem."""
from typing import Set, Iterator, Tuple, Dict, Optional, Union
from collections import defaultdict
import itertools

import logging
import pddl

from .utils.pddl import ground_formula, ground_term
from .utils.graph import subtypes_closure
from .model.effect import Effect

LOGGER = logging.getLogger(__name__)


class GroundedAction:

    """Planning Action.

    :param action: input PDDL action
    :param assignment: action arguments as a dict of variable -> object
    """

    def __init__(self,
                 action: Union[pddl.Action, pddl.Task],
                 assignment: Dict[str, str]):
        self.__name = action.name
        self.__assignment = assignment

        # Action instance
        self.__repr = ground_term(self.name,
                                  map(lambda x: x.name,
                                      action.parameters),
                                  assignment.__getitem__)

        if isinstance(action, pddl.Action):
            # Preconditions
            self.__positive_pre = set()
            self.__negative_pre = set()
            ground_formula(action.precondition,
                           self.__assignment.__getitem__,
                           self.__positive_pre,
                           self.__negative_pre)
            # Effects
            self.__effects = set()
            addlit = set()
            dellit = set()
            ground_formula(action.effect, self.__assignment.__getitem__,
                           addlit, dellit, self.__effects)
            self.__effects.add(Effect(frozenset(), frozenset(), addlit, dellit))
        elif isinstance(action, pddl.Task):
            pass

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
        LOGGER.debug("is %s applicable in %s?", repr(self), state)
        return (self.__positive_pre <= state
                and
                self.__negative_pre.isdisjoint(state)
                )

    def apply(self, state: Set[str]) -> Set[str]:
        """Apply action to state and return a new state."""
        LOGGER.debug("apply %s to %s:", repr(self), state)
        positive = set()
        negative = set()
        for eff in self.effects:
            pos, neg = eff.applicable(state)
            positive |= pos
            negative |= neg
        LOGGER.debug("literals to add: %s", positive)
        LOGGER.debug("literals to del: %s", negative)
        new_state = (state | positive) - negative
        LOGGER.debug("result in %s", new_state)
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
        # Objects
        self.__types_subtypes = subtypes_closure(domain.types)
        self.__objects_per_type = defaultdict(set)
        for obj in domain.constants:
            self.__objects_per_type[obj.type].add(obj.name)
        for obj in problem.objects:
            self.__objects_per_type[obj.type].add(obj.name)
        # Actions
        profile(self.__ground_action)
        ground = self.__ground_action
        self.__actions = {repr(ga): ga
                          for action in domain.actions
                          for ga in self.__ground_action(action)}
        # Tasks
        self.__tasks = {repr(gt): gt
                        for task in domain.tasks
                        for gt in ground(task)}
        # Methods

        # Initial state
        self.__init = frozenset(ground_term(lit.name, lit.arguments)
                                for lit in problem.init)
        LOGGER.debug("initial state: %s", self.__init)
        # Goal state
        self.__positive_goal = set()
        self.__negative_goal = set()
        ground_formula(problem.goal, lambda x: x,
                       self.__positive_goal, self.__negative_goal)
        LOGGER.debug("goal state: %s and NOT %s", self.__positive_goal, self.__negative_goal)

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
    def goal(self) -> Tuple[Set[str], Set[str]]:
        """Get goal state. Maybe be ((), ()) if the problem is defined by a Task Network."""
        return self.__positive_goal, self.__negative_goal

    @property
    def actions(self) -> Iterator[GroundedAction]:
        """Returns an iterator over the actions."""
        return self.__actions.values()

    @property
    def tasks(self) -> Iterator[GroundedAction]:
        return self.__tasks.values()

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

    def __ground_action(self, action: pddl.Action) -> Iterator[GroundedAction]:
        """Ground an action."""
        variables = [itertools.product([param.name],
                                       self.objects_of(param.type))
                     for param in action.parameters]
        for assignment in itertools.product(*variables):
            yield GroundedAction(action, dict(assignment))
