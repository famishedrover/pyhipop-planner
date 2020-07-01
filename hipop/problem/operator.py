from typing import Union, Set, Tuple, Dict, Iterator, Optional
from abc import ABC
import logging
from collections import defaultdict

import pddl

from ..utils.pddl import ground_term, loop_over_predicates
from ..utils.poset import Poset

LOGGER = logging.getLogger(__name__)


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
                 precondition: Union[pddl.AtomicFormula, pddl.NotFormula, pddl.AndFormula],
                 assignment: Optional[Dict[str, str]],
                 predicates,
                 static_literals):

        pos = defaultdict(set)
        for formula in loop_over_predicates(precondition, negative=False):
            term = ground_term(formula.name, formula.arguments,
                               (assignment.__getitem__ if assignment else (lambda x: x)))
            pos[formula.name].add(term)

        def check_pos(predicate, literals):
            return ((predicate not in predicates)
                    and
                    not (literals < static_literals))
        if any((check_pos(k, v) for k, v in pos.items())):
            raise GroundingImpossibleError(precondition, assignment)

        self.__positive_pre = frozenset(x for pred, literals in pos.items()
                                        for x in literals
                                        if pred in predicates)

        neg = defaultdict(set)
        for formula in loop_over_predicates(precondition, positive=False):
            term = ground_term(formula.name, formula.arguments,
                               (assignment.__getitem__ if assignment else (lambda x: x)))
            neg[formula.name].add(term)

        def check_neg(predicate, literals):
            return ((predicate not in predicates)
                    and
                    (literals < static_literals))
        if any((check_neg(k, v) for k, v in neg.items())):
            raise GroundingImpossibleError(precondition, assignment)

        self.__negative_pre = frozenset(x for pred, literals in neg.items()
                                        for x in literals
                                        if pred in predicates)

    @property
    def preconditions(self) -> Tuple[Set[str], Set[str]]:
        """Get preconditions as a pair of (positive pre, negative pre)."""
        return (self.__positive_pre, self.__negative_pre)

    def is_applicable(self, state: Set[str]) -> bool:
        """Test if operator is applicable in state."""
        return (self.__positive_pre <= state
                and
                self.__negative_pre.isdisjoint(state)
                )


class WithEffect(ABC):

    """An operator with one effect.

    :param effect: operator effect formula
    :param assignment: operator arguments as a dict of variable -> object
    """

    def __init__(self,
                 effect: pddl.AndFormula,
                 assignment: Dict[str, str]):

        assign = (assignment.__getitem__ if assignment else (lambda x: x))
        self.__add_effect = frozenset(ground_term(formula.name,
                                                  formula.arguments,
                                                  assign)
                                      for formula in loop_over_predicates(effect, negative=False))
        self.__del_effect = frozenset(ground_term(formula.name,
                                                  formula.arguments,
                                                  assign)
                                      for formula in loop_over_predicates(effect, positive=False))

    @property
    def effect(self) -> Tuple[Set[str], Set[str]]:
        """Get add/del effects."""
        return self.__add_effect, self.del_effect

    def apply(self, state: Set[str]) -> Set[str]:
        """Apply operator to state and return a new state."""
        #LOGGER.debug("apply %s to %s:", repr(self), state)
        #LOGGER.debug("literals to add: %s", self.__add_effect)
        #LOGGER.debug("literals to del: %s", self.__del_effect)
        new_state = (state - self.__del_effect) | self.__add_effect
        #LOGGER.debug("result in %s", new_state)
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
                 predicates,
                 static_literals):
        GroundedOperator.__init__(self, action, assignment)
        WithPrecondition.__init__(self, action.precondition, assignment,
                                   predicates, static_literals)
        WithEffect.__init__(self, action.effect, assignment)
        self.__cost = 1

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
                 predicates,
                 static_literals):
        GroundedOperator.__init__(self, method, assignment)
        WithPrecondition.__init__(self, method.precondition, assignment,
                                   predicates, static_literals)
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
        self.__network.close()
        self.__is_method = True

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
                 predicates,
                 static_literals):
        GroundedOperator.__init__(self, task, assignment)
        self.__methods = dict()

    def add_method(self, method: GroundedMethod) -> bool:
        if not (self.name in method.task):
            LOGGER.warning("Method %s does not refine task %s! method.task is %s", method.name, self.name, method.task)
            return False
        self.__methods[str(method)] = method
        return True

    def remove_method(self, method: str):
        del self.__methods[method]

    def get_method(self, method: str) -> GroundedMethod:
        return self.__methods[method]

    @property
    def methods(self) -> Iterator[GroundedMethod]:
        return self.__methods.values()
