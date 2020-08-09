"""Planning Problem."""
from typing import Set, Iterator, Tuple, Dict, Optional, Union, Any, Type, List
from collections import defaultdict
from functools import reduce
import itertools
import math
import logging
import networkx

import pddl

from .objects import Objects
from .literals import Literals
from .operator import GroundedOperator, GroundedAction
from .hadd import HAdd
from .errors import GroundingImpossibleError

from ..utils.pddl import iter_objects

LOGGER = logging.getLogger(__name__)


class Problem:

    """Planning Problem.

    The planning problem is grounded

    :param problem: PDDL problem
    :param domain: PDDL domain
    """

    def __init__(self, problem: pddl.Problem, domain: pddl.Domain):
        # Objects
        self.__objects = Objects(problem=problem, domain=domain)
        # Literals
        self.__literals = Literals(problem=problem, domain=domain, 
                                   objects=self.__objects)
        # TODO: '=' predicate
        # TODO: 'sortof' predicate
        # TODO: goal state

        LOGGER.debug("PDDL actions: %s", " ".join(
            (a.name for a in domain.actions)))
        LOGGER.debug("PDDL methods: %s", " ".join(
            (a.name for a in domain.methods)))
        LOGGER.debug("PDDL tasks: %s", " ".join(
            (a.name for a in domain.tasks)))

        # TODO: goal HTN

        # Actions grounding
        LOGGER.info("Possible action groundings: %d", self.__nb_grounded_actions(domain.actions))
        ground = self.ground_operator
        grounded_actions = {str(a): a for action in domain.actions
                             for a in ground(action, GroundedAction, dict())}
        LOGGER.info("Grounded actions: %d", len(grounded_actions))
        
        # H-Add

        self.__hadd = HAdd(grounded_actions.values(),
                           self.__literals.init[0],
                           self.__literals.varying
                           )
        LOGGER.info("Reachable actions: %d", sum(
            1 for a in grounded_actions if not math.isinf(self.__hadd(a))))

    def ground_operator(self, op: Any, gop: type,
                        assignments: Dict[str, str]) -> Iterator[Type[GroundedOperator]]:
        """Ground an action."""
        for assignment in iter_objects(op.parameters, self.__objects.per_type, assignments):
            try:
                LOGGER.debug("grounding %s on variables %s", op.name, assignment)
                yield gop(op, dict(assignment), literals=self.__literals, objects=self.__objects)
            except GroundingImpossibleError as ex:
                LOGGER.debug("droping operator %s : %s", op.name, ex.message)
                pass

    def __nb_grounded_actions(self, actions):
        per_type = self.__objects.per_type
        nb_groundings = 0
        for action in actions:
            n = reduce(int.__mul__, [len(list(per_type(p.type))) for p in action.parameters])
            LOGGER.debug("action %s has %d groundings", action.name, n)
            nb_groundings += n
        return nb_groundings
