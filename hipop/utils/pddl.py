import logging
import pddl

from ..model.effect import Effect

LOGGER = logging.getLogger(__name__)


def pythonize(pddl_str: str) -> str:
    return pddl_str.replace('-', '_')


def ground_term(fun, args, assignment=lambda x: x):
    arguments = " ".join(map(assignment, args))
    return f"({fun} {arguments})"


def loop_over_predicates(formula, positive=True, negative=True, conditional=False):
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

def get_forall(formula):
    if isinstance(formula, pddl.AndFormula):
        for lit in formula.formulas:
            yield from get_forall(lit)
    elif isinstance(formula, pddl.ForallFormula):
        yield formula
