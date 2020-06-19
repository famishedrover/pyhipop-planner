from typing import Union, Set, Tuple, Dict, Iterator, Optional
from abc import ABC
import logging

import pddl

from ..utils.pddl import ground_formula, ground_term
from ..utils.poset import Poset
from .effect import Effect

LOGGER = logging.getLogger(__name__)


class WithPreconditions(ABC):

    """An operator with preconditions.

    :param precondition: operator precondition formula
    :param assignment: operator arguments as a dict of variable -> object
    """

    def __init__(self,
                 precondition: Union[pddl.AtomicFormula, pddl.NotFormula, pddl.AndFormula],
                 assignment: Optional[Dict[str, str]]):

            self.__positive_pre = set()
            self.__negative_pre = set()
            ground_formula(precondition,
                           (assignment.__getitem__ if assignment else (lambda x: x)),
                           self.__positive_pre,
                           self.__negative_pre)

    @property
    def preconditions(self) -> Tuple[Set[str], Set[str]]:
        """Get preconditions as a pair of (positive pre, negative pre)."""
        return (self.__positive_pre, self.__negative_pre)

    def is_applicable(self, state: Set[str]) -> bool:
        """Test if operator is applicable in state."""
        LOGGER.debug("is %s applicable in %s?", repr(self), state)
        return (self.__positive_pre <= state
                and
                self.__negative_pre.isdisjoint(state)
                )


class WithEffects(ABC):

    """An operator with effects.

    :param effect: operator effect formula
    :param assignment: operator arguments as a dict of variable -> object
    """

    def __init__(self,
                 effect: pddl.AndFormula,
                 assignment: Dict[str, str]):

         self.__effects = set()
         addlit = set()
         dellit = set()
         ground_formula(effect,
                        (assignment.__getitem__ if assignment else (lambda x: x)),
                        addlit, dellit, self.__effects)
         self.__effects.add(Effect(frozenset(), frozenset(), addlit, dellit))

    @property
    def effects(self) -> Set[Effect]:
        """Get set of effects."""
        return self.__effects

    def apply(self, state: Set[str]) -> Set[str]:
        """Apply operator to state and return a new state."""
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


class GroundedOperator(ABC):

    """A Grounded Operator.

    :param operator: input PDDL operator
    :param assignment: operator arguments as a dict of variable -> object
    """

    def __init__(self,
                 operator: Union[pddl.Action, pddl.Task, pddl.Method],
                 assignment: Dict[str, str]):

        self.__name = operator.name
        self._assignment = assignment
        # Grounded name
        self.__repr = ground_term(self.name,
                                  map(lambda x: x.name,
                                      operator.parameters),
                                  (assignment.__getitem__ if assignment else (lambda x: x)))

    def __repr__(self):
        return self.__repr

    @property
    def name(self) -> str:
        """Get operator name."""
        return self.__name


class GroundedAction(WithPreconditions, WithEffects, GroundedOperator):

    """Planning Action.

    :param action: input PDDL action
    :param assignment: action arguments as a dict of variable -> object
    """

    def __init__(self,
                 action: pddl.Action,
                 assignment: Dict[str, str]):
        GroundedOperator.__init__(self, action, assignment)
        WithPreconditions.__init__(self, action.precondition, assignment)
        WithEffects.__init__(self, action.effect, assignment)
        self.__cost = 1

    @property
    def cost(self) -> int:
        """Get action name."""
        return self.__cost


class GroundedMethod(WithPreconditions, GroundedOperator):

    """Planning Hierarchical Method.

    :param method: input PDDL method
    :param assignment: method arguments as a dict of variable -> object
    """

    def __init__(self,
                 method: pddl.Method,
                 assignment: Optional[Dict[str, str]] = None):
        GroundedOperator.__init__(self, method, assignment)
        WithPreconditions.__init__(self, method.precondition, assignment)
        assign = assignment.__getitem__ if assignment else (lambda x: x)
        self.__subtasks = dict()
        for taskid, task in method.network.subtasks:
            self.__subtasks[taskid] = ground_term(task.name,
                                                  task.arguments,
                                                  assign)
        self.__task = ground_term(method.task.name,
                                  method.task.arguments,
                                  assign)
        self.__network = Poset()
        for task, relation in method.network.ordering.items():
            self.__network.add(task, relation, label=self.__subtasks[task])
        self.__network.close()

    @property
    def task(self) -> str:
        return self.__task

    @property
    def task_network(self) -> Poset:
        return self.__network

    def subtask(self, taskid: str) -> str:
        return self.__subtasks[taskid]


class GroundedTask(GroundedOperator):

    """Planning Hierarchical Task.

    :param task: input PDDL task
    :param assignment: task arguments as a dict of variable -> object
    """

    def __init__(self,
                 task: pddl.Task,
                 assignment: Dict[str, str]):
        GroundedOperator.__init__(self, task, assignment)
        self.__methods = dict()

    def add_method(self, method: GroundedMethod) -> bool:
        if not (self.name in method.task):
            LOGGER.warning("Method %s does not refine task %s! method.task is %s", method.name, self.name, method.task)
            return False
        self.__methods[repr(method)] = method
        return True

    @property
    def methods(self) -> Iterator[GroundedMethod]:
        return self.__methods.values()
