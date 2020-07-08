from typing import Iterable, Dict, Any, Union, List, Tuple
import itertools
import logging
import pddl

LOGGER = logging.getLogger(__name__)
GOAL = Union[pddl.AndFormula, pddl.AtomicFormula,
             pddl.ForallFormula, pddl.NotFormula,
             pddl.WhenEffect]

def pythonize(pddl_str: str) -> str:
    return pddl_str.replace('-', '_')


def ground_term(fun: Any, args: Iterable[Any], assignment=lambda x: x):
    arguments = " ".join(((assignment(x) if x[0] == '?' else x) for x in args))
    return f"({fun} {arguments})"


def loop_over_predicates(formula: GOAL, positive: bool = True,
                         negative : bool = True,
                         conditional : bool = False) -> Iterable[pddl.AtomicFormula]:
    if isinstance(formula, pddl.AtomicFormula) and positive:
        yield formula
    elif isinstance(formula, pddl.NotFormula) and negative:
        yield formula.formula
    elif isinstance(formula, pddl.AndFormula):
        for lit in formula.formulas:
            yield from loop_over_predicates(lit, positive, negative)
    elif isinstance(formula, pddl.WhenEffect) and conditional:
        yield (formula.condition, loop_over_predicates(formula.effect,
                                                       positive, negative,
                                                       conditional))


def iter_objects(variables: Iterable[pddl.Type], objects) -> Iterable[List[Tuple[str, List[str]]]]:
    var_assign = [itertools.product([var.name],
                                    objects[var.type])
                  for var in variables]
    return itertools.product(*var_assign)
