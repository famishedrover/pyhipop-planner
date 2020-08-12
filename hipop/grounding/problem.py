"""Planning Problem."""
from typing import Set, Iterator, Tuple, Dict, Optional, Union, Any, Type, List
from collections import defaultdict
from functools import reduce
import itertools
import math
import logging
import networkx
import networkx.drawing.nx_pydot as pydot
import time

import pddl

from .errors import GroundingImpossibleError, RequirementMissing, RequirementNotSupported, TypingAssignmentInconsistent
from .objects import Objects, iter_objects
from .literals import Literals
from .operator import GroundedOperator, GroundedAction, GroundedMethod, GroundedTask
from .hadd import HAdd
from .tdg import TaskDecompositionGraph
from .atoms import Atoms
from .logic import FalseExpr

LOGGER = logging.getLogger(__name__)


class Problem:

    """Planning Problem.

    The planning problem is grounded

    :param problem: PDDL problem
    :param domain: PDDL domain
    """

    def __init__(self, problem: pddl.Problem, domain: pddl.Domain, 
                 output: Optional[str] = None,
                 filter_rigid: bool = True,
                 filter_relaxed: bool = True,
                 pure_htn: bool = True,
                 remove_contradictory_effects: bool = True):
        # Attributes
        self.__problem = problem.name
        self.__domain = domain.name
        self.__remove_contradictory_effects = remove_contradictory_effects
        # Requirements
        self.__check_requirements(domain)
        if self.__typing:
            LOGGER.info("Domain uses typing")
        if self.__equality:
            LOGGER.info("Domain uses '=' predicate")
        if self.__method_precondition:
            LOGGER.info("Domain uses method preconditions")
        # Objects
        self.__objects = Objects(problem=problem, domain=domain)
        if output is not None:
            self.__objects.write_dot(f"{output}types-hierarchy.dot", with_objects=False)
            self.__objects.write_dot(f"{output}types-objects.dot", with_objects=True)
        # Literals
        self.__literals = Literals(problem=problem, domain=domain, 
                                   objects=self.__objects,
                                   filter_rigid=filter_rigid,
                                   equality=self.__equality)
        # TODO: goal state

        # Goal task
        if problem.htn:
            tasks = list(domain.tasks) + [pddl.Task('__top')]
            methods = list(domain.methods) + [problem.htn]
        else:
            tasks = domain.tasks
            methods = domain.methods

        # Actions grounding
        LOGGER.info("PDDL actions: %d", len(domain.actions))
        LOGGER.info("Possible action groundings: %d",
                    self.__nb_grounded_operators(domain.actions))
        ground = self.__ground_operator
        tic = time.process_time()
        self.__grounded_actions = dict()
        for action in domain.actions:
            groundings = list(ground(action, GroundedAction, dict()))
            LOGGER.debug("operator %s has %d groundings", action.name, len(groundings))
            self.__grounded_actions.update({str(a): a for a in groundings})
        toc = time.process_time()
        LOGGER.info("action grounding duration: %.3fs", (toc - tic))
        LOGGER.info("Grounded actions: %d", len(self.__grounded_actions))

        # TODO: Actions mutexes
        
        # H-Add
        tic = time.process_time()
        self.__hadd = HAdd(self.__grounded_actions.values(),
                           self.__literals.init[0],
                           self.__literals.varying
                           )
        toc = time.process_time()
        LOGGER.info("hadd duration: %.3fs", (toc - tic))
        if output is not None:
            self.__hadd.write_dot(f"{output}hadd-graph.dot")
        LOGGER.info("Reachable actions: %d", sum(
            1 for a in self.__grounded_actions if not math.isinf(self.__hadd(a))))

        # Methods grounding
        LOGGER.info("PDDL methods: %d", len(methods))
        LOGGER.info("Possible method groundings: %d",
                    self.__nb_grounded_operators(methods))
        ground = self.__ground_operator
        tic = time.process_time()
        self.__grounded_methods = dict()
        for op in methods:
            groundings = list(ground(op, GroundedMethod, dict()))
            LOGGER.debug("operator %s has %d groundings",
                         op.name, len(groundings))
            self.__grounded_methods.update({str(a): a for a in groundings})
        toc = time.process_time()
        LOGGER.info("method grounding duration: %.3fs", (toc - tic))
        LOGGER.info("Grounded methods: %d", len(self.__grounded_methods))

        # Tasks grounding
        LOGGER.info("PDDL tasks: %d", len(tasks))
        LOGGER.info("Possible task groundings: %d",
                    self.__nb_grounded_operators(tasks))
        ground = self.__ground_operator
        tic = time.process_time()
        self.__grounded_tasks = dict()
        for op in tasks:
            groundings = list(ground(op, GroundedTask, dict()))
            LOGGER.debug("operator %s has %d groundings",
                         op.name, len(groundings))
            self.__grounded_tasks.update({str(a): a for a in groundings})
        toc = time.process_time()
        LOGGER.info("task grounding duration: %.3fs", (toc - tic))
        LOGGER.info("Grounded tasks: %d", len(self.__grounded_tasks))

        # Lifted TDG
        # TODO: move to tdg.py
        lifted_tdg = networkx.DiGraph()
        for m in methods:
            lifted_tdg.add_edge(m.task.name, m.name)
            for (_, t) in m.network.subtasks:
                lifted_tdg.add_edge(m.name, t.name)
        pydot.write_dot(lifted_tdg, "tdg-lifted.dot")
        # TODO: we can first filter on the lifted TDG! even including action not reachable in delete-relaxation

        # TDG
        tic = time.process_time()
        self.__tdg = TaskDecompositionGraph(
            self.__grounded_actions, self.__grounded_methods, self.__grounded_tasks)
        toc = time.process_time()
        LOGGER.info("initial TDG duration: %.3fs", (toc - tic))
        LOGGER.info("TDG initial: %d", len(self.__tdg))
        if output is not None:
            self.__tdg.write_dot(f"{output}tdg-initial.dot")
        # Remove useless nodes
        tic = time.process_time()
        if filter_relaxed:
            self.__tdg.remove_useless(
                (a for a in self.__grounded_actions if math.isinf(self.__hadd(a))))
        else:
            self.__tdg.remove_useless(())
        toc = time.process_time()
        LOGGER.info("TDG filtering duration: %.3fs", (toc - tic))
        LOGGER.info("TDG minimal: %d", len(self.__tdg))
        if output is not None:
            self.__tdg.write_dot(f"{output}tdg-minimal.dot")
        # Keep only HTN decomposition
        if problem.htn and pure_htn:
            tic = time.process_time()
            self.__tdg.htn('(__top )')
            toc = time.process_time()
            LOGGER.info("TDG HTN filtering duration: %.3fs", (toc - tic))
            LOGGER.info("TDG HTN: %d", len(self.__tdg))
            if output is not None:
                self.__tdg.write_dot(f"{output}tdg-htn.dot")

    @property
    def name(self) -> str:
        return self.__problem

    @property
    def init(self) -> Tuple[Set[int], Set[int]]:
        return self.__literals.init

    @property
    def tdg(self) -> TaskDecompositionGraph:
        return self.__tdg

    def action(self, name: str) -> GroundedAction:
        return self.__grounded_actions[name]

    def method(self, name: str) -> GroundedMethod:
        return self.__grounded_methods[name]

    def task(self, name: str) -> GroundedTask:
        return self.__grounded_tasks[name]

    def has_action(self, name: str) -> bool:
        return name in self.__grounded_actions

    def has_method(self, name: str) -> bool:
        return name in self.__grounded_methods

    def has_task(self, name: str) -> bool:
        return name in self.__grounded_tasks

    def __ground_operator(self, op: Any, gop: type,
                        assignments: Dict[str, str]) -> Iterator[Type[GroundedOperator]]:
        """Ground an operator."""
        def f_extract(l, formula):
            if formula.name in self.__literals.rigid_relations:
                return l + formula.arguments
            return l
        try:
            vars_in_rigid = set(self.__literals.extract(f_extract, op.precondition))
        except AttributeError:
            vars_in_rigid = set()
        #LOGGER.debug("%s: %d parameters; %d used in rigid relations", op.name, len(op.parameters), len(vars_in_rigid))

        rigid_params = [p for p in op.parameters if p.name in vars_in_rigid]

        def f_rigid(x, *args):
            if x in self.__literals.rigid_relations:
                return Atoms.atom(x, *args)[0]
            else:
                return f'{x}{args}'

        if rigid_params:
            for rigid_assign in iter_objects(rigid_params, self.__objects.per_type, assignments):
                expr = self.__literals.build_partial(op.precondition, dict(rigid_assign), self.__objects, f_rigid)
                #LOGGER.debug("%s partial rigid pre: %s", op.name, expr)
                expr = expr.simplify(*self.__literals.rigid_literals)
                #LOGGER.debug("%s partial rigid simplified pre: %s", op.name, expr)
                if isinstance(expr, FalseExpr):
                    #LOGGER.debug("droping operator %s for impossible rigid grounding", op.name)
                    continue
                for assignment in iter_objects(op.parameters, self.__objects.per_type, rigid_assign):
                    try:
                        yield gop(op, dict(assignment), literals=self.__literals, objects=self.__objects,
                            remove_contradictory_effects=self.__remove_contradictory_effects)
                    except GroundingImpossibleError as ex:
                        #LOGGER.debug("droping operator %s : %s [%s]", op.name, ex.message, ex.__class__.__name__)
                        pass
        else:
            for assignment in iter_objects(op.parameters, self.__objects.per_type, assignments):
                try:
                    yield gop(op, dict(assignment), literals=self.__literals, objects=self.__objects,
                              remove_contradictory_effects=self.__remove_contradictory_effects)
                except GroundingImpossibleError as ex:
                    LOGGER.debug(
                        "droping operator %s : %s [%s]", op.name, ex.message, ex.__class__.__name__)

    def __nb_grounded_operators(self, operators):
        per_type = self.__objects.per_type
        nb_groundings = 0
        for op in operators:
            n = reduce(int.__mul__, [len(list(per_type(p.type))) for p in op.parameters], 1)
            LOGGER.debug("operator %s has %d groundings", op.name, n)
            nb_groundings += n
        return nb_groundings

    def __check_requirements(self, domain: pddl.Domain):
        self.__typing = (':typing' in domain.requirements)
        self.__equality = (':equality' in domain.requirements)
        self.__method_precondition = (
            ':method-precondition' in domain.requirements) or (':method-preconditions' in domain.requirements)

        for req in [':disjunctive-preconditions',
                    ':existential-preconditions',
                    ':quantified-preconditions',
                    ':conditional-effects',
                    ':fluents',
                    ':adl',
                    ':durative-actions',
                    ':duration-inequalities',
                    ':continuous-effects']:
            if req in domain.requirements:
                raise RequirementNotSupported(req)
