from abc import ABC
from typing import Union, Dict, Iterable
#from pyeda.boolalg.expr import Expression, exprvar, Not, And, ITE
from collections import defaultdict
import pddl
import logging

LOGGER = logging.getLogger(__name__)
GOAL = Union[pddl.AndFormula, pddl.AtomicFormula,
             pddl.ForallFormula, pddl.NotFormula,
             pddl.WhenEffect]


class Expression(ABC):
    def evaluate(self, trues):
        pass
    @classmethod
    def build_expression(cls, formula: GOAL,
                         assignment: Dict[str,str],
                         objects: Dict[str,Iterable[str]]) -> 'Expression':
        if isinstance(formula, pddl.AtomicFormula):
            return Atom(Literals.literal(formula.name,
                                         *[assignment[a]
                                           for a in formula.arguments]))
        if isinstance(formula, pddl.NotFormula):
            return Not(cls.build_expression(formula.formula, assignment, objects))
        if isinstance(formula, pddl.AndFormula):
            return And(*[cls.build_expression(f, assignment, objects)
                         for f in formula.formulas])
        if isinstance(formula, pddl.WhenEffect):
            return ITE(cls.build_expression(formula.condition, assignment, objects),
                       cls.build_expression(formula.effect, assignment, objects),
                       False)
        if isinstance(formula, pddl.ForallFormula):
            return And(*[cls.build_expression(formula.goal,
                                          dict(assign, **assignment),
                                          objects)
                         for assign in iter_objects(formula.variables, objects)])


class TrueExpr(Expression):
    def evaluate(self, trues):
        return True

class FalseExpr(Expression):
    def evaluate(self, trues):
        return False

class Atom(Expression):
    def __init__(self, proposition):
        self.__proposition = proposition
    def evaluate(self, trues):
        return self.__proposition in trues

class And(Expression):
    def __init__(self, *expressions):
        self.__expressions = expressions
    def evaluate(self, trues):
        return all((e.evaluate(trues) for e in self.__expressions))

class Not(Expression):
    def __init__(self, expression):
        self.__expression = expression
    def evaluate(self, trues):
        return not self.__expression.evaluate(trues)


class Literals:
    __literals = defaultdict(dict)
    __literal_counter = 0

    @classmethod
    def literal(cls, predicate: str, *arguments) -> Expression:
        args = tuple(arguments)
        if args not in cls.__literals[predicate]:
            cls.__literals[predicate][args] = cls.__literal_counter
            LOGGER.debug("Add literal %s %s: %s", predicate, args,
                         cls.__literals[predicate][args])
            cls.__literal_counter += 1
        return cls.__literals[predicate][args]

    @classmethod
    def literals_of(cls, predicate: str) -> Iterable[Expression]:
        return cls.__literals[predicate].values()
