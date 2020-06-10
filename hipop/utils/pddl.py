import pddl
import logging
logger = logging.getLogger(__name__)

def pythonize(pddl_str: str) -> str:
    return pddl_str.replace('-', '_')

def ground_term(fun, args, assignment=lambda x: x):
    arguments = " ".join(map(lambda p: assignment(p), args))
    return f"({fun} {arguments})"

def ground_formula(formula, assignment, pos, neg, effects=set()):
    logger.debug(f"grounding formula {formula}")
    if isinstance(formula, pddl.AtomicFormula):
        pos.add(ground_term(formula.name,
                            formula.arguments,
                            assignment.__getitem__))
    elif isinstance(formula, pddl.NotFormula):
        neg.add(ground_term(formula.formula.name,
                            formula.formula.arguments,
                            assignment.__getitem__))
    elif isinstance(formula, pddl.AndFormula):
        for lit in formula.formulas:
            ground_formula(lit, assignment, pos, neg)
    elif isinstance(formula, pddl.WhenEffect):
        condpos, condneg, addlit, dellit = set(), set(), set(), set()
        ground_formula(formula.condition, assignment, condpos, condneg)
        ground_formula(formula.effect, assignment, addlit, dellit)
        effects.add(Effect(condpos, condneg, addlit, dellit))
