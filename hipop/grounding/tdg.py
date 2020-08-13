import logging
from typing import Optional, Dict, Iterator, Tuple, Set
from collections import defaultdict, namedtuple, deque
import networkx
import math

from .operator import GroundedOperator, GroundedAction, GroundedTask, GroundedMethod
from .hadd import HAdd

LOGGER = logging.getLogger(__name__)

TDGHeuristic = namedtuple('h_TDG', ['cost', 'modifications', 'hadd_max'])

class TaskDecompositionGraph:

    def __init__(self, actions: Dict[str, GroundedAction],
                 methods: Dict[str, GroundedMethod],
                 tasks: Dict[str, GroundedTask],
                 hadd: HAdd):

        self.__graph = networkx.DiGraph()
        self.__useless = set()

        self.__graph.add_nodes_from(tasks, type='task')
        self.__graph.add_nodes_from(methods, shape='rectangle', type='method')
        self.__graph.add_nodes_from(actions, type='action')
        for name, method in methods.items():
            if method.task in tasks:
                self.__graph.add_edge(method.task, name)
            else:
                LOGGER.debug("USELESS: method %s has no task %s", name, method.task)
                self.__useless.add(name)
            for subtask in method.subtasks:
                if subtask in tasks or subtask in actions:
                    self.__graph.add_edge(name, subtask)
                else:
                    LOGGER.debug("USELESS: method %s has no subtask %s",
                                name, subtask)
                    self.__useless.add(name)
        #LOGGER.info("TDG cycles: %d", len(list(networkx.simple_cycles(self.__graph))))
        # TODO: prune cycles (see Behnke et al., 2020)

        # Optimistic task effects (see Angelic Planning) and Heuristics
        self.__task_effects = defaultdict(lambda: (set(), set()))
        self.__hadd = hadd
        self.__heuristics = defaultdict(lambda: TDGHeuristic(0, 0, math.inf))
        for name, action in actions.items():
            self.__task_effects[name] = action.effect
            self.__heuristics[name] = TDGHeuristic(cost=action.cost, modifications=1, 
                                                   hadd_max=sum(self.__hadd(a) for x in action.support for a in x))
            self.__graph.nodes[name]['label'] = f"{name}\n{self.__heuristics[name]}"

    def __len__(self):
        return self.__graph.number_of_nodes()

    def __iter__(self):
        return self.__graph.__iter__()

    def successors(self, node: str) -> Iterator[str]:
        return self.__graph.successors(node)

    def task_effects(self, task: str) -> Tuple[Set[int], Set[int]]:
        return self.__task_effects[task]

    def heuristics(self, node: str) -> TDGHeuristic:
        return self.__heuristics[node]

    def remove_useless(self, useless: Iterator[str]):
        LOGGER.debug("Initialy useless: %d", len(self.__useless))
        self.__useless |= set(useless)
        LOGGER.debug("Added useless: %d", len(self.__useless))
        reverse_scc = networkx.condensation(self.__graph).reverse()
        sorted_scc = deque(networkx.topological_sort(reverse_scc))
        while sorted_scc:
            scc = sorted_scc.popleft()
            members = reverse_scc.nodes[scc]['members']
            # TODO: we should iterate until fix point
            for node in members:
                if node in self.__useless:
                    continue
                if self.__graph.nodes[node]['type'] == 'action':
                    continue

                # Methods
                elif self.__graph.nodes[node]['type'] == 'method':
                    if any(x in self.__useless for x in self.__graph.successors(node)):
                        LOGGER.debug("Pruning %s: some subtask is useless", node)
                        self.__useless.add(node)
                        continue
                    else:
                        # Compute task effects and heuristics
                        adds, dels = set(), set()
                        h_c, h_m, h_add = 0, 0, 0
                        for subtask in self.__graph.successors(node):
                            a, d = self.__task_effects[subtask]
                            adds |= a
                            dels |= d
                            h_c += self.__heuristics[subtask].cost
                            h_m += self.__heuristics[subtask].modifications
                            h_add += self.__heuristics[subtask].hadd_max
                        self.__task_effects[node] = (adds, dels)

                # Tasks
                elif self.__graph.nodes[node]['type'] == 'task':
                    if all(x in self.__useless for x in self.__graph.successors(node)):
                        LOGGER.debug("Pruning %s: all methods are useless", node)
                        self.__useless.add(node)
                        continue
                    else:
                        # Compute task effects and heuristics
                        adds, dels = set(), set()
                        h_c, h_m, h_add = math.inf, math.inf, 0
                        for method in self.__graph.successors(node):
                            a, d = self.__task_effects[method]
                            adds |= a
                            dels |= d
                            h_c = min(h_c, self.__heuristics[method].cost)
                            h_m = min(h_m, self.__heuristics[method].modifications)
                            h_add = max(h_add, self.__heuristics[subtask].hadd_max)
                        self.__task_effects[node] = (adds, dels)

                self.__heuristics[node] = TDGHeuristic(cost=h_c, modifications=h_m, hadd_max=h_add)
                self.__graph.nodes[node]['label'] = f"{node}\n{self.__heuristics[node]}"

        LOGGER.debug("Recursively useless: %d", len(self.__useless))
        self.__graph.remove_nodes_from(self.__useless)

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
