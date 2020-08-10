"""Planning Problem."""
from typing import Set, Iterator, Tuple, Dict, Optional, Union, Any, Type, List
from collections import defaultdict
from functools import reduce
import itertools
import math
import logging
import networkx
import time

import pddl

from .errors import GroundingImpossibleError, RequirementMissing, RequirementNotSupported
from .objects import Objects
from .literals import Literals
from .operator import GroundedOperator, GroundedAction, GroundedMethod, GroundedTask
from .hadd import HAdd
from .tdg import TaskDecompositionGraph

from ..utils.pddl import iter_objects

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
                 pure_htn: bool = True):
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
            self.__objects.write_dot(f"{output}types-hierarchy.dot")
        # Literals
        self.__literals = Literals(problem=problem, domain=domain, 
                                   objects=self.__objects,
                                   filter_rigid=filter_rigid)
        # TODO: '=' predicate
        # TODO: 'sortof' predicate
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
        grounded_actions = {str(a): a for action in domain.actions
                             for a in ground(action, GroundedAction, dict())}
        toc = time.process_time()
        LOGGER.info("action grounding duration: %.3fs", (toc - tic))
        LOGGER.info("Grounded actions: %d", len(grounded_actions))
        
        # H-Add
        tic = time.process_time()
        self.__hadd = HAdd(grounded_actions.values(),
                           self.__literals.init[0],
                           self.__literals.varying
                           )
        toc = time.process_time()
        LOGGER.info("hadd duration: %.3fs", (toc - tic))
        if output is not None:
            self.__hadd.write_dot(f"{output}hadd-graph.dot")
        LOGGER.info("Reachable actions: %d", sum(
            1 for a in grounded_actions if not math.isinf(self.__hadd(a))))

        # Methods grounding
        LOGGER.info("PDDL methods: %d", len(methods))
        LOGGER.info("Possible method groundings: %d",
                    self.__nb_grounded_operators(methods))
        ground = self.__ground_operator
        tic = time.process_time()
        grounded_methods = {str(grounded_op): grounded_op for op in methods
                            for grounded_op in ground(op, GroundedMethod, dict())}
        toc = time.process_time()
        LOGGER.info("method grounding duration: %.3fs", (toc - tic))
        LOGGER.info("Grounded methods: %d", len(grounded_methods))

        # Tasks grounding
        LOGGER.info("PDDL tasks: %d", len(tasks))
        LOGGER.info("Possible task groundings: %d",
                    self.__nb_grounded_operators(tasks))
        ground = self.__ground_operator
        tic = time.process_time()
        grounded_tasks = {str(grounded_op): grounded_op for op in tasks
                            for grounded_op in ground(op, GroundedTask, dict())}
        toc = time.process_time()
        LOGGER.info("task grounding duration: %.3fs", (toc - tic))
        LOGGER.info("Grounded tasks: %d", len(grounded_tasks))

        # TDG
        tic = time.process_time()
        tdg = TaskDecompositionGraph(
            grounded_actions, grounded_methods, grounded_tasks)
        toc = time.process_time()
        LOGGER.info("initial TDG duration: %.3fs", (toc - tic))
        LOGGER.info("TDG initial: %d", len(tdg))
        if output is not None:
            tdg.write_dot(f"{output}tdg-initial.dot")
        tic = time.process_time()
        if filter_relaxed:
            tdg.remove_useless((a for a in grounded_actions if math.isinf(self.__hadd(a))))
        else:
            tdg.remove_useless(())
        toc = time.process_time()
        LOGGER.info("TDG filtering duration: %.3fs", (toc - tic))
        LOGGER.info("TDG minimal: %d", len(tdg))
        if output is not None:
            tdg.write_dot(f"{output}tdg-minimal.dot")
        if problem.htn and pure_htn:
            tic = time.process_time()
            tdg.htn('(__top )')
            toc = time.process_time()
            LOGGER.info("TDG HTN filtering duration: %.3fs", (toc - tic))
            LOGGER.info("TDG HTN: %d", len(tdg))
            if output is not None:
                tdg.write_dot(f"{output}tdg-htn.dot")

    def __ground_operator(self, op: Any, gop: type,
                        assignments: Dict[str, str]) -> Iterator[Type[GroundedOperator]]:
        """Ground an action."""
        for assignment in iter_objects(op.parameters, self.__objects.per_type, assignments):
            try:
                LOGGER.debug("grounding %s on variables %s", op.name, assignment)
                yield gop(op, dict(assignment), literals=self.__literals, objects=self.__objects)
            except GroundingImpossibleError as ex:
                LOGGER.debug("droping operator %s : %s", op.name, ex.message)
                pass

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
