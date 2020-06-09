"""Planning Problem."""

from typing import Set, Iterator, Tuple, List, Dict
import logging
logger = logging.getLogger(__name__)

from collections import defaultdict
import itertools
import pddl

from .utils.pddl import pythonize
from .utils.graph import subtypes_closure


class GroundedAction:

    """Planning Action.

    :param action: input PDDL action
    :param assignment: action arguments as a dict of variable -> object
    """

    def __init__(self,
                 action: pddl.Action,
                 assignment: Dict[str, str]):
        self.__name = action.name
        arguments = " ".join(map(lambda p: assignment[p.name],
                           action.parameters))
        self.__repr = f"({self.name} {arguments})"

        self.__pddl_action = action
        self.__assignment = assignment

    def __repr__(self):
        return self.__repr

    @property
    def name(self) -> str:
        """Get action name."""
        return self.__name

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

        for action in domain.actions:
            for ga in self.__ground_action(action):
                logger.info(f"grounded action {ga}")

    @property
    def name(self) -> str:
        """Problem name."""
        return self.__name

    @property
    def domain(self) -> str:
        """Domain name."""
        return self.__domain

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

    def __ground_action(self, action: pddl.Action) -> Iterator[GroundedAction]:
        """Ground an action."""
        variables = []
        for param in action.parameters:
            variables.append([(param.name, obj)
                              for obj in self.objects_of(param.type)])
        for assignment in itertools.product(*variables):
            yield GroundedAction(action, dict(assignment))
