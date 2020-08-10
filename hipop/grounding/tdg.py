import logging
from typing import Optional, Dict, Iterator
from collections import defaultdict, namedtuple, deque
import networkx
import math

from .operator import GroundedOperator, GroundedAction, GroundedTask, GroundedMethod
from .hadd import HAdd

LOGGER = logging.getLogger(__name__)

TDGHeuristic = namedtuple('h_TDG', ['cost', 'modifications'])

class TaskDecompositionGraph:

    def __init__(self, actions: Dict[str, GroundedAction],
                 methods: Dict[str, GroundedMethod],
                 tasks: Dict[str, GroundedTask]):

        self.__graph = networkx.DiGraph()
        #self.__heuristic = defaultdict(lambda: TDGHeuristic(0, 0))
        #self.__task_effects = defaultdict(lambda: (set(), set()))
        self.__useless = set()

        self.__graph.add_nodes_from(tasks, type='task')
        self.__graph.add_nodes_from(methods, shape='rectangle', type='method')
        self.__graph.add_nodes_from(actions, type='action')
        for name, method in methods.items():
            if method.task in tasks:
                self.__graph.add_edge(method.task, name)
            else:
                self.__useless.add(name)
            for subtask in method.subtasks:
                if subtask in tasks or subtask in actions:
                    self.__graph.add_edge(name, subtask)
                else:
                    self.__useless.add(name)

    def __len__(self):
        return self.__graph.number_of_nodes()

    def __iter__(self):
        return self.__graph.__iter__()

    def remove_useless(self, useless: Iterator[str]):
        LOGGER.debug("Initialy useless: %d", len(self.__useless))
        self.__useless |= set(useless)
        LOGGER.debug("Added useless: %d", len(self.__useless))
        reverse_graph = self.__graph.reverse()
        sorted_nodes = deque(networkx.topological_sort(reverse_graph))
        while sorted_nodes:
            node = sorted_nodes.popleft()
            if node in self.__useless:
                pass
            elif self.__graph.nodes[node]['type'] == 'method':
                if any(x in self.__useless for x in self.__graph.successors(node)):
                    self.__useless.add(node)
            elif self.__graph.nodes[node]['type'] == 'task':
                if all(x in self.__useless for x in self.__graph.successors(node)):
                    self.__useless.add(node)
        LOGGER.debug("Recursively useless: %d", len(self.__useless))
        for node in self.__useless:
            self.__graph.remove_node(node)

    def htn(self, root_task: str):
        reachables = networkx.single_source_shortest_path_length(self.__graph, root_task)
        unreachables = [n for n in self.__graph.nodes if n not in reachables]
        self.__graph.remove_nodes_from(unreachables)
        for u in unreachables:
            self.__useless.discard(u)

    def write_dot(self, filename: str):
        import networkx.drawing.nx_pydot as pydot
        for u in self.__useless:
            if u in self.__graph.nodes:
                self.__graph.nodes[u]['color'] = 'red'
                self.__graph.nodes[u]['style'] = 'filled'
        pydot.write_dot(self.__graph, filename)
