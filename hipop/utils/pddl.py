from typing import Iterable, Callable, Dict, Any, Union, List, Tuple
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

def iter_objects(variables: Iterable[pddl.Type], 
                 objects: Callable[[str], List[str]],
                 assignment: Dict[str, str]) -> Iterable[List[Tuple[str, List[str]]]]:
    var_assign = []
    for var in variables:
        if var.name in assignment:
            assigns = [(var.name, assignment[var.name])]
        else:
            assigns = itertools.product([var.name], objects(var.type))
        var_assign.append(assigns)
    return itertools.product(*var_assign)
