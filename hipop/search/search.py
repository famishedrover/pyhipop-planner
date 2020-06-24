import sys
import logging
from ..utils.graph import INFTY
from hipop.problem.problem import Problem
from queue import PriorityQueue
from .heuristics import Heuristic

logger = logging.getLogger(__name__)

class PlanNode(object):
    def __init__(self, value, children = []):
        self.value = value
        self.children = children

    def __str__(self, level=0):
        ret = "\t"*level+repr(self.value)+"\n"
        for child in self.children:
            ret += child.__str__(level+1)
        return ret

    def __repr__(self):
        return str(self.value)


class Plan(object):

    def __init__(self, value = None):
        if value == None:
            self.__root = value
        else:
            self.set_root(value)

    def set_root(self, value):
        self.__root = PlanNode(value)

    def __repr__(self):
        return self.__root.__str__(0)

    def is_empty(self):
        return self.__root == None


class SHOP():

    def __init__(self, problem):
        self.__plan = Plan()
        self.__problem = problem

    @property
    def plan(self):
        return self.__plan

    @property
    def problem(self):
        return self.__problem

    def find_plan(self, state, tasks) -> bool:
        """
         Searches for a plan that accomplishes tasks in state.
         Basically performs a DFS.
        :param state: Initial state of the search
        :return:   If successful, return True. Otherwise return False.
        """
        self.plan.set_root('Init')
        seen = []
        seen.append(state)
        result = self.seek_plan(state, tasks, list(), 0, seen)
        logger.info("SHOP plan found: %s", result)
        print (self.plan)
        return result

    def seek_plan(self, state, tasks, branch, depth, seen):
        logger.debug("state: %s", state)
        logger.debug("tasks: %s", tasks)
        logger.debug("depth: %d", depth)
        logger.debug("current branch: %s", branch)
        if tasks == []:
            logger.debug("returning plan: %s", branch)
            return branch
        current_task = tasks[0]
        result = False

        try:
            # first op is a compount task
            task = self.problem.get_task(current_task)
            for method in task.methods:
                logger.debug("depth %d : method %s", depth, method)
                for subtask_name in method.sorted_tasks:
                    logger.debug(" - %s", subtask_name)
                subtasks = list(method.sorted_tasks)
                logger.debug("# subtasks: {}".format(len(subtasks)))
                if subtasks:
                    logger.debug("depth %d : diggin into subtasks", depth)
                    result = self.seek_plan(state,
                                            subtasks + tasks[1:],
                                            branch,
                                            depth+1,
                                            seen)
                else:
                    logger.debug("depth %d - method %s has NO subtasks", depth, method.name)
                    result = self.seek_plan(state,
                                            tasks[1:],
                                            branch,
                                            depth+1,
                                            seen)

                if result:
                    break

        except KeyError:
            # primitive task, aka Action
            action = self.problem.get_action(current_task)
            logger.debug("depth %d action %s", depth, action)
            if action.is_applicable(state):
                s1 = action.apply(state)
                if s1 in seen:
                    return False # branch.append(action.name)
                if s1:
                    seen.append(s1)
                    result = self.seek_plan(s1,
                                            tasks[1:],
                                            branch + [action],
                                            depth+1,
                                            seen)
            else:
                logger.debug("Action {} is NOT applicable".format(action))
                return False

        return result




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

    def __lt__(self, other):
        """
        Defines a sorting order
        :param other: Node to compare to
        :return:
        """
        return self.fn < other.fn

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
        return self.goal <= n.state and self.problem.goal[1].isdisjoint(n.state)


    def best_first_search(self):
        open_list = BFSPriorityQueue()
        # todo check which python queue is more efficient for searching in closed list
        closed_list = []
        open_list.put(self.root)

        g = None
        while not open_list.empty():
            n = open_list.get()
            logger.debug("expanding {}".format(n))
            if n in closed_list:
                logger.debug("already visited")
                del n
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
                    # if next in open_list.queue:
                    #     # todo 1) should compare states 2) replace in open_list the node with lower fn
                    #     # todo: look for an iterable Priorityqueue
                    #     # todo: in any case, if a node is already in open it doesn't matter: they will be sorted accordingly to the heuristic value
                    #     logger.debug("Node already in the Open List")
                    #     continue
                    open_list.put(next)
                    # open_list.sort(key=lambda s: s.fn)
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
