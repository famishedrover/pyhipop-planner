from typing import Union, Set, Tuple, Dict, Iterator, Iterable, Optional
from abc import ABC
import logging
from collections import defaultdict

import pddl
from ..utils.pddl import ground_term, loop_over_predicates
from ..utils.logic import Literals, TrueExpr, Expression, FalseExpr
from ..utils.poset import Poset

LOGGER = logging.getLogger(__name__)

GOAL = Union[pddl.AndFormula, pddl.AtomicFormula,
             pddl.ForallFormula, pddl.NotFormula,
             pddl.WhenEffect]


class GroundingImpossibleError(Exception):
    def __init__(self, predicates, assignment):
        self.__predicates = predicates
        self.__assignment = assignment

    @property
    def message(self):
        return f"Grounding of {self.__predicates} impossible for {self.__assignment}"


class WithPrecondition(ABC):

    """An operator with preconditions.

    :param precondition: operator precondition formula
    :param assignment: operator arguments as a dict of variable -> object
    """

    def __init__(self,
                 precondition: Optional[GOAL],
                 assignment: Dict[str, str],
                 static_literals: Set[int],
                 static_predicates: Set[str],
                 objects: Dict[str, Iterable[str]]):

        if not precondition:
            self._pre = TrueExpr()
        else:
            self._pre = Expression.build_expression(precondition, assignment, objects)
            self._pre = self._pre.simplify(static_literals, static_predicates)
            if isinstance(self._pre, FalseExpr):
                raise GroundingImpossibleError(precondition, assignment)
        self.__pos, self.__neg = self._pre.effect

    @property
    def precondition(self) -> Expression:
        """Get precondition expression."""
        return self._pre

    @property
    def is_tautology(self) -> bool:
        return isinstance(self._pre, TrueExpr)

    @property
    def is_contradiction(self) -> bool:
        return isinstance(self._pre, FalseExpr)

    @property
    def support(self) -> Tuple[Set[int], Set[int]]:
        """Get precondition expression."""
        return self.__pos, self.__neg

    def is_applicable(self, state: Set[int]) -> bool:
        """Test if operator is applicable in state."""
        if self.is_tautology:
            return True
        if self.is_contradiction:
            return False
        return (self.__pos <= state) and not (bool(self.__neg) and self.__neg <= state)
        #return self._pre.evaluate(state)


class WithEffect(ABC):

    """An operator with one effect.

    :param effect: operator effect formula
    :param assignment: operator arguments as a dict of variable -> object
    """

    def __init__(self,
                 effect: pddl.AndFormula,
                 assignment: Dict[str, str],
                 objects):

        self.__effect = Expression.build_expression(effect, assignment, objects)
        self.__adds, self.__dels = self.__effect.effect
        inconsistent = self.__adds & self.__dels
        if inconsistent:
            raise GroundingImpossibleError(str(self), inconsistent)

    @property
    def effect(self) -> Tuple[Set[str], Set[str]]:
        """Get effect expression."""
        return self.__adds, self.__dels

    def apply(self, state: Set[int]) -> Set[int]:
        """Apply operator to state and return a new state."""
        new_state = (state - self.__dels) | self.__adds
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
        self.__pddl = operator
        self.__is_method = False
        # Grounded name
        self.__repr = ground_term(self.name,
                                  map(lambda x: x.name,
                                      operator.parameters),
                                  (assignment.__getitem__ if assignment else (lambda x: x)))

    def __str__(self):
        return self.__repr

    def __repr__(self):
        return self.__repr

    @property
    def pddl(self):
        return self.__pddl

    @property
    def is_method(self):
        return self.__is_method

    @property
    def name(self) -> str:
        """Get operator name."""
        return self.__name

    @property
    def assignment(self):
        return self._assignment

class GroundedAction(WithPrecondition, WithEffect, GroundedOperator):

    """Planning Action.

    :param action: input PDDL action
    :param assignment: action arguments as a dict of variable -> object
    """

    def __init__(self,
                 action: pddl.Action,
                 assignment: Dict[str, str],
                 static_literals, static_predicates,
                 objects: Dict[str, Iterable[str]]):
        GroundedOperator.__init__(self, action, assignment)
        WithPrecondition.__init__(self, action.precondition, assignment,
                                  static_literals, static_predicates,
                                  objects)
        WithEffect.__init__(self, action.effect, assignment, objects)
        self.__cost = 1
        LOGGER.debug("action %s pre %s eff %s", str(self), self.precondition, self.effect)

    @property
    def cost(self) -> int:
        """Get action name."""
        return self.__cost


class GroundedMethod(WithPrecondition, GroundedOperator):

    """Planning Hierarchical Method.

    :param method: input PDDL method
    :param assignment: method arguments as a dict of variable -> object
    """

    def __init__(self,
                 method: pddl.Method,
                 assignment: Optional[Dict[str, str]],
                 static_literals, static_predicates,
                 objects: Dict[str, Iterable[str]]):
        GroundedOperator.__init__(self, method, assignment)
        WithPrecondition.__init__(self, method.precondition, assignment,
                                  static_literals, static_predicates,
                                  objects)
        assign = assignment.__getitem__ if assignment else (lambda x: x)

        self.__subtasks = dict()
        self.__network = Poset()

        self.__task = ground_term(method.task.name,
                                  method.task.arguments,
                                  assign)

        for taskid, task in method.network.subtasks:
            self.__subtasks[taskid] = ground_term(task.name,
                                                  task.arguments,
                                                  assign)
            self.__network.add(taskid, [])

        for task, relation in method.network.ordering.items():
            self.__network.add_relation(task, relation, check_poset=False)
        #self.__network.close()
        self.__is_method = True
        LOGGER.debug("method %s pre %s", str(self), self.precondition)

    @property
    def task(self) -> str:
        return self.__task

    @property
    def task_network(self) -> Poset:
        return self.__network

    @property
    def subtasks(self) -> Iterator[str]:
        return self.__subtasks.values()

    def subtask(self, taskid: str) -> str:
        return self.__subtasks[taskid]

    @property
    def sorted_tasks(self) -> Iterator[str]:
        return map(self.subtask, self.task_network.topological_sort())

class GroundedTask(GroundedOperator):

    """Planning Hierarchical Task.

    :param task: input PDDL task
    :param assignment: task arguments as a dict of variable -> object
    """

    def __init__(self,
                 task: pddl.Task,
                 assignment: Dict[str, str],
                 **kwargs):
        GroundedOperator.__init__(self, task, assignment)
        self.__methods = dict()
        LOGGER.debug("task %s", str(self))

    def add_method(self, method: GroundedMethod) -> bool:
        if not (self.name in method.task):
            LOGGER.warning("Method %s does not refine task %s! method.task is %s", method.name, self.name, method.task)
            return False
        self.__methods[str(method)] = method
        LOGGER.debug("Task %s has method %s", self, method)
        return True

    def remove_method(self, method: str):
        del self.__methods[method]

    def get_method(self, method: str) -> GroundedMethod:
        return self.__methods[method]

    @property
    def methods(self) -> Iterator[GroundedMethod]:
        return self.__methods.values()
