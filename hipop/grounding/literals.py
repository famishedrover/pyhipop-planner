from typing import Tuple, Iterator, List, Dict, Any, Callable, Set
from collections import defaultdict
import logging

import pddl

from ..utils.pddl import iter_objects
from ..utils.logic import GOAL, Atom, Not, And, TrueExpr, FalseExpr, Expression
from .atoms import Atoms
from .objects import Objects

LOGGER = logging.getLogger(__name__)


class Literals:
    def __init__(self, domain: pddl.Domain, problem: pddl.Problem, objects: Objects):
        # Build all Atoms
        atoms_per_predicate = defaultdict(set)
        for predicate in domain.predicates:
            for args in iter_objects(predicate.variables, objects.per_type, {}):
                atom, _ = Atoms.atom(predicate.name, *[a[1] for a in args])
                atoms_per_predicate[predicate.name].add(atom)
            LOGGER.debug("predicate %s: %s", predicate.name, atoms_per_predicate[predicate.name])
        LOGGER.info("Predicates: %d", len(atoms_per_predicate))
        LOGGER.info("Atoms: %d", len(Atoms.atoms()))
        # Fluents
        fluents = set()
        for action in domain.actions:
            expr = self.__build_expression(action.effect, {}, objects,
                                           lambda x, *args: x)
            pos, neg = expr.support
            fluents |= pos
            fluents |= neg
        LOGGER.info("Fluents: %d", len(fluents))
        LOGGER.debug("Fluents: %s", fluents)
        rigid = set(pred.name for pred in domain.predicates) - fluents
        LOGGER.info("Rigid relations: %d", len(rigid))
        LOGGER.debug("Rigid relations: %s", rigid)
        # Rigid Literals
        rigid_atoms = set(a for a in Atoms.atoms() if Atoms.atom_to_predicate(a)[0] in rigid)
        LOGGER.info("Rigid atoms: %d", len(rigid_atoms))
        LOGGER.debug("Rigid atoms: %s", rigid_atoms)
        pb_init = set(Atoms.atom(lit.name, *lit.arguments)[0] for lit in problem.init)
        LOGGER.debug("Problem init state: %s", pb_init)
        self.__rigid_literals = (
            pb_init & rigid_atoms), (rigid_atoms - pb_init)
        LOGGER.info("Rigid literals: %d", sum(map(len, self.__rigid_literals)))
        LOGGER.debug("Rigid literals: %s", self.__rigid_literals)
        # Init State
        self.__init_literals = (
            pb_init - self.__rigid_literals[0]), (Atoms.atoms() - rigid_atoms - pb_init)
        LOGGER.info("Init state literals: %d", sum(
            map(len, self.__init_literals)))
        LOGGER.debug("Init state literals: %s", self.__init_literals)

    @property
    def rigid_literals(self) -> Tuple[Set[int], Set[int]]:
        return self.__rigid_literals

    @property
    def varying(self) -> Set[int]:
        a, b = self.__init_literals
        return a | b

    @property
    def init(self) -> Tuple[Set[int], Set[int]]:
        return self.__init_literals

    def __assign(self, args: List[str], assignment: Dict[str, str], 
                 complete: bool = True) -> List[str]:
        result = []
        for a in args:
            if a[0] == '?':
                # a is a variable
                if a in assignment:
                    result.append(assignment[a])
                elif complete:
                    raise KeyError()
                else:
                    result.append(a)
            else:
                # a is a constant
                result.append(a)
        return result

    def __build_expression(self, formula: GOAL,
                         assignment: Dict[str, str],
                         objects: Dict[str, Iterator[str]],
                         atom_factory: Callable[[List[str]], Any]) -> Expression:
        if isinstance(formula, pddl.AtomicFormula):
            atom = atom_factory(formula.name, 
                                *self.__assign(formula.arguments,
                                             assignment, False))
            return Atom(atom)
        if isinstance(formula, pddl.NotFormula):
            return Not(self.__build_expression(formula.formula, assignment, objects, atom_factory))
        if isinstance(formula, pddl.AndFormula):
            return And(*[self.__build_expression(f, assignment, objects, atom_factory)
                        for f in formula.formulas])
        if isinstance(formula, pddl.WhenEffect):
            LOGGER.error("conditional effects not supported!")
            return FalseExpr()
        if isinstance(formula, pddl.ForallFormula):
            return And(*[self.__build_expression(formula.goal,
                                dict(assign, **assignment),
                objects, atom_factory)
                     for assign in iter_objects(formula.variables, objects, dict())])
        return TrueExpr()

    def build(self, formula: GOAL,
              assignment: Dict[str, str],
              objects: Dict[str, Iterator[str]]):
        def atom_factory(x, *args):
            a, _ = Atoms.atom(x, *args)
            return a
        return self.__build_expression(formula, assignment, objects, atom_factory)
