from typing import Optional
import logging
import math
from collections import deque
import enum

from ..grounding.problem import Problem
from ..plan.plan import HierarchicalPartialPlan

LOGGER = logging.getLogger(__name__)


class TreeSearchAlgorithm(enum.Enum):
    BFS = "bfs"
    DFS = "dfs"


class TreeSearch:
    """Breadth-First Search.

    procedure BFS(G, root) is
        let Q be a queue
        label root as discovered
        Q.enqueue(root)
        while Q is not empty do
            v := Q.dequeue()
            if v is the goal then
                return v
            for all edges from v to w in G.adjacentEdges(v) do
                if w is not labeled as discovered then
                    label w as discovered
                    w.parent := v
                    Q.enqueue(w)
    """
    def __init__(self, problem: Problem):
        self.__problem = problem
        # queue structures
        self.__Q = deque()
        self.__discovered = []
        # initial plan
        plan = HierarchicalPartialPlan(problem, init=True)
        if self.__problem.has_root_task():
            root = self.__problem.root_task()
            plan.add_task(root)
        self.__Q.append(plan)
        self.__discovered.append(plan)

    def solve(self,
              algorithm: TreeSearchAlgorithm = TreeSearchAlgorithm.BFS,
              output_current_plan: bool = True, 
              output_new_plans: bool = True) -> Optional[HierarchicalPartialPlan]:

        while self.__Q:
            if algorithm == TreeSearchAlgorithm.BFS:
                v = self.__Q.popleft()
            elif algorithm == TreeSearchAlgorithm.DFS:
                v = self.__Q.pop()

            LOGGER.info("current plan: %d", id(v))
            if output_current_plan:
                v.write_dot('current-plan.dot')
            if not v.has_flaws():
                LOGGER.info("solution found")
                return v
            LOGGER.info("flaws: AF=%d, OL=%d, Th=%d", 
                        len(v.abstract_flaws),
                        len(v.open_links),
                        0)

            children = []
            prune = False

            # loop over abstract flaws
            for flaw in v.abstract_flaws:
                resolvers = list(v.abstract_flaw_resolvers(flaw))
                LOGGER.debug("Resolvers for flaw %s: %d", 
                            flaw, len(resolvers))
                if not resolvers:
                    prune = True
                    break
                for w in resolvers:
                    LOGGER.debug("- new plan %d", id(w))
                    if output_new_plans:
                        w.write_dot(f'plan-{id(v)}.dot')
                    children.append(w)

            # loop over open links
            for flaw in v.open_links:
                if prune: break
                resolvers = list(v.open_link_resolvers(flaw))
                LOGGER.debug("Resolvers for flaw %s: %d",
                             flaw, len(resolvers))
                if not resolvers and not v.abstract_flaws:
                    prune = True
                    break
                for w in resolvers:
                    LOGGER.debug("- new plan %d", id(w))
                    if output_new_plans:
                        w.write_dot(f'plan-{id(v)}.dot')
                    children.append(w)

            if prune:
                LOGGER.debug("deadend: pruning")
                continue

            # successors
            for w in children:
                self.__Q.append(w)
                self.__discovered.append(w)

            LOGGER.info("Q size: %d", len(self.__Q))
            LOGGER.info("Discovered size: %d", len(self.__discovered))
