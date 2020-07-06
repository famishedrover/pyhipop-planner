from typing import Union, Dict, Iterable
from pyeda.boolalg.expr import Expression, exprvar, Not, And, ITE
from collections import defaultdict
import itertools
import pddl
import logging

LOGGER = logging.getLogger(__name__)
GOAL = Union[pddl.AndFormula, pddl.AtomicFormula,
             pddl.ForallFormula, pddl.NotFormula,
             pddl.WhenEffect]


class Literals:
    __literals = defaultdict(dict)

    @classmethod
    def literal(cls, predicate: str, *arguments) -> Expression:
        args = tuple(arguments)
        if args not in cls.__literals[predicate]:
            i = len(cls.__literals[predicate])
            cls.__literals[predicate][args] = exprvar(predicate, i)
            LOGGER.debug("Add literal %s %s: %s", predicate, args,
                         cls.__literals[predicate][args])
        return cls.__literals[predicate][args]

    @classmethod
    def literals_of(cls, predicate: str) -> Iterable[Expression]:
        return cls.__literals[predicate].values()


def iter_objects(variables: Iterable[pddl.Type],
                 objects: Dict[str,Iterable[str]]):
    var_assign = [itertools.product([var.name],
                                    objects[var.type])
                  for var in variables]
    return itertools.product(*var_assign)


def build_expression(formula: GOAL,
                     assignment: Dict[str,str],
                     objects: Dict[str,Iterable[str]]) -> Expression:
    if isinstance(formula, pddl.AtomicFormula):
        return Literals.literal(formula.name,
                                *[assignment[a] for a in formula.arguments])
    if isinstance(formula, pddl.NotFormula):
        return Not(build_expression(formula.formula, assignment, objects))
    if isinstance(formula, pddl.AndFormula):
        return And(*[build_expression(f, assignment, objects)
                     for f in formula.formulas])
    if isinstance(formula, pddl.WhenEffect):
        return ITE(build_expression(formula.condition, assignment, objects),
                   build_expression(formula.effect, assignment, objects),
                   False)
    if isinstance(formula, pddl.ForallFormula):
        return And(*[build_expression(formula.goal,
                                      dict(assign, **assignment),
                                      objects)
                     for assign in iter_objects(formula.variables, objects)])
