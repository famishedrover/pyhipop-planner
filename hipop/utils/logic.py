from abc import ABC
from typing import Union, Dict, Iterable, Tuple
from collections import defaultdict
import pddl
import logging

from .pddl import iter_objects

LOGGER = logging.getLogger(__name__)
GOAL = Union[pddl.AndFormula, pddl.AtomicFormula,
             pddl.ForallFormula, pddl.NotFormula,
             pddl.WhenEffect]


class Expression(ABC):
    def evaluate(self, trues):
        LOGGER.error("not implemented")
    def simplify(self, trues, falses):
        LOGGER.error("not implemented")
    @property
    def support(self):
        LOGGER.error("not implemented")
    def __repr__(self):
        return str(self)
    @classmethod
    def build_expression(cls, formula: GOAL,
                         assignment: Dict[str,str],
                         objects: Dict[str,Iterable[str]]) -> 'Expression':
        #LOGGER.debug("build: %s %s", formula, type(formula))
        if isinstance(formula, pddl.AtomicFormula):
            return Atom(Literals.literal(formula.name,
                                         *[(assignment[a] if a[0] == '?' else a)
                                           for a in formula.arguments]))
        if isinstance(formula, pddl.NotFormula):
            return Not(cls.build_expression(formula.formula, assignment, objects))
        if isinstance(formula, pddl.AndFormula):
            return And(*[cls.build_expression(f, assignment, objects)
                         for f in formula.formulas])
        if isinstance(formula, pddl.WhenEffect):
            LOGGER.error("conditional effects not supported!")
            return FalseExpr()
        if isinstance(formula, pddl.ForallFormula):
            return And(*[cls.build_expression(formula.goal,
                                          dict(assign, **assignment),
                                          objects)
                         for assign in iter_objects(formula.variables, objects, dict())])
        return TrueExpr()


class TrueExpr(Expression):
    def evaluate(self, trues):
        return True
    def simplify(self, trues, falses):
        return TrueExpr()
    @property
    def support(self):
        return set(), set()
    def __str__(self):
        return 'T'

class FalseExpr(Expression):
    def evaluate(self, trues):
        return False
    def simplify(self, trues, falses):
        return FalseExpr()
    @property
    def support(self):
        return set(), set()
    def __str__(self):
        return 'F'

class Atom(Expression):
    def __init__(self, proposition):
        self.__proposition = proposition
    def evaluate(self, trues):
        return self.__proposition[0] in trues
    def simplify(self, trues, falses):
        if self.__proposition[0] in trues:
            #LOGGER.debug("prop %s: %d must be in %s", self.__proposition, self.__proposition[0], trues)
            return TrueExpr()
        if self.__proposition[0] in falses:
            #LOGGER.debug("prop %s: %s must not be in %s", self.__proposition, self.__proposition[0], falses)
            return FalseExpr()
        return self
    @property
    def support(self):
        return set({self.__proposition[0]}), set()
    def __str__(self):
        return f"[{self.__proposition}]"

class And(Expression):
    def __init__(self, *expressions):
        self.__expressions = expressions
    def evaluate(self, trues):
        return all((e.evaluate(trues) for e in self.__expressions))
    def simplify(self, trues, falses):
        exprs = set(e.simplify(trues, falses) for e in self.__expressions)
        if any((isinstance(e, FalseExpr) for e in exprs)):
            return FalseExpr()
        if all((isinstance(e, TrueExpr) for e in exprs)):
            return TrueExpr()
        return And(*exprs)
    @property
    def support(self):
        adds = set()
        dels = set()
        for e in self.__expressions:
            a, d = e.support
            adds |= a
            dels |= d
        return adds, dels
    def __str__(self):
        return f"({'&'.join(map(str, self.__expressions))})"


class Not(Expression):
    def __init__(self, expression):
        self.__expression = expression
    def evaluate(self, trues):
        return not self.__expression.evaluate(trues)
    def simplify(self, trues, falses):
        expr = self.__expression.simplify(falses, trues)
        if isinstance(expr, TrueExpr):
            return TrueExpr()
        if isinstance(expr, FalseExpr):
            return FalseExpr()
        return self
    @property
    def support(self):
        adds, dels = self.__expression.support
        return dels, adds
    def __str__(self):
        return f"(~{self.__expression})"


class Literals:
    __literals = defaultdict(dict)
    __predicates = defaultdict(dict)
    __literal_counter = 0

    @classmethod
    def literals(cls):
        return cls.__predicates.keys()

    @classmethod
    def literal(cls, predicate: str, *arguments) -> Tuple[int, str]:
        args = tuple(arguments)
        if args not in cls.__literals[predicate]:
            cls.__literals[predicate][args] = (cls.__literal_counter, predicate)
            cls.__predicates[cls.__literal_counter] = f"{predicate} {args}"
            LOGGER.debug("Add literal %s %s: %s", predicate, args,
                         cls.__literals[predicate][args])
            cls.__literal_counter += 1
        return cls.__literals[predicate][args]

    @classmethod
    def literals_of(cls, predicate: str) -> Iterable[Tuple[int, str]]:
        return cls.__literals[predicate].values()

    @classmethod
    def lit_to_predicate(cls, lit: int) -> str:
        return cls.__predicates[lit]
