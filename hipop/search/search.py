import sys
import logging
from ..utils.graph import INFTY
from hipop.problem.problem import Problem
from queue import PriorityQueue
from .heuristics import Heuristic

logger = logging.getLogger(__name__)



class SearchNode:
    def __init__(self, state=frozenset()):
        self.state = state
        self.__hn = INFTY
        self.__gn = 0
        self.__fn = INFTY
        self.__op =  None # action
        self.__father = None # None for root

    def __eq__(self, other):
        """
        Compares two search nodes
        :param other: node to compare to
        :return: True if same state, False if not or if None
        """
        try:
            return self.state == other.state
        except AttributeError:
            return False

    def __hash__(self):
        return hash(self.state)

    def __repr__(self):
        return repr('Node: ' + self.state.__str__() + ' f=' + str(self.fn) + ', h=' + str(self.hn) )

    @property
    def father(self) :
        """Get father of the node."""
        return self.__father

    @property
    def hn(self) -> int:
        """Get heuristic value of the node."""
        return self.__hn

    def set_father(self, f) :
        """Get father of the node."""
        self.__father = f

    def set_hn(self, heur):
        """Get heuristic value of the node."""
        if heur > INFTY:
            self.__hn = INFTY
        else:
            self.__hn = heur

    def set_gn(self, heur):
        """Get heuristic value of the node."""
        self.__gn = heur

    def set_fn(self, heur):
        """Get heuristic value of the node."""
        if heur > INFTY:
            self.__fn = INFTY
        else:
            self.__fn = heur

    @property
    def fn(self) -> int:
        """Get heuristic value of the node."""
        return self.__fn

    @property
    def gn(self) -> int:
        """Get heuristic value of the node."""
        return self.__gn

    @property
    def op(self) -> int:
        """Get the operator leading to the node from the father."""
        return self.__op

    def set_op(self, op):
        """Set the operator leading to the node from the father."""
        self.__op = op

    def evaluate(self, heuristic):
        """Calculates the heuristic value
            :returns infty if goal is not reachable"""
        self.set_hn(heuristic.compute(self))
        logger.debug("H-value: {} {}".format(self.hn, self.state))
        if self.father != None:
            self.set_gn(self.father.gn + self.op.cost)
        else:
            # Root case
            pass
        self.set_fn(self.gn + self.hn)


class GBFPriorityQueue(PriorityQueue):
    """
    This class simply orders the PriorityQueue by hn value
    """
    def __init__(self, maxsize=0):
        super().__init__(maxsize)

    def put(self, x):
        super().put((x.hn, x))

    def get(self):
        return super().get()[1]
    

class BFSPriorityQueue(PriorityQueue):
    """
    This class simply orders the PriorityQueue by fn value
    """
    def __init__(self, maxsize=0):
        super().__init__(maxsize)

    def put(self, x):
        super().put((x.fn, x))

    def get(self):
        return super().get()[1]


class Search:
    def __init__(self, problem: Problem, heuristic: Heuristic):
        self.__problem = problem
        self.heuristic = heuristic
        self.__root = SearchNode(problem.init)

    def set_root(self, r):
        self.__root = r

    @property
    def root(self) -> SearchNode:
        """Get the root node."""
        return self.__root

    @property
    def problem(self) :
        """Get the planning problem."""
        return self.__problem

    @property
    def goal(self) :
        """Get the planning problem.
            :returns forzenset
        """
        return self.problem.goal_state

    @property
    def failed(self) -> bool:
        """Search failed."""
        return False

    def goal_achieved(self, n: SearchNode):
        """
        TODO: we are considering here only valid states
            thus no negative literals

        :param n: Current SearchNode
        :return: True if the goal is reached
        """
        return self.goal == n.state

    def best_first_search(self):
        # todo: to be transformed in PriorityQueue
        open_list = []
        # todo check which python queue is more efficient for searching like closed list
        closed_list = []
        open_list.append(self.root)

        g = None
        while len(open_list) > 0:
            n = open_list.pop()
            logger.debug("expanding {}".format(n))
            if n in closed_list:
                logger.debug("already visited")
                continue
            closed_list.append(n)
            if self.goal_achieved(n):
                logger.info("Goal reached")
                g = n
                break
            for o in self.problem.actions:
                if o.is_applicable(n.state):
                    next = SearchNode()
                    next.set_op(o)
                    next.set_father(n)
                    next.state = o.apply(n.state)
                    logger.debug("Adding {} to Open List".format(next.state))
                    next.evaluate(self.heuristic)
                    if next.hn == INFTY:
                        continue
                    if next in open_list:
                        # todo 1) should compare states 2) replace in open_list the node with lower fn
                        logger.debug("Node already in the Open List")
                        continue
                    open_list.append(next)
                    open_list.sort(key=lambda s: s.fn)
        return g


    def execute_search(self, n):
        # todo: depends on the algorithm selected, add more options
        # BFS
        return self.best_first_search()

    def solve(self):
        g = None
        plan_found = False
        self.root.evaluate(self.heuristic)
        if self.root.hn < INFTY:
            g = self.execute_search(g)
        else:
            g = None

        if g == None:
            self.__failed = True
            logger.info("No plan found")
            return plan_found
        else:
            plan_found = True

        plan = []
        _ = g
        while _ != None and _.op != None:
            plan.append(_.op)
            _ = _.father

        logger.info("Plan found")
        for k in plan:
              logger.info("  {}".format(k))
        logger.info("Plan lenght:  {}".format(len(plan)))

        return plan_found



