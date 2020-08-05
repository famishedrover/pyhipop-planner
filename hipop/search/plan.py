from collections import defaultdict, namedtuple
from typing import Union, Any, Iterator, Optional, Iterable, Set
from copy import deepcopy, copy
import math
import logging
import networkx
from operator import itemgetter, attrgetter
from sortedcontainers import SortedKeyList

import pddl
from ..utils.poset import Poset, IncrementalPoset
from ..utils.logic import Literals
from ..problem.problem import Problem
from ..problem.operator import GroundedMethod, GroundedTask, GroundedAction
from ..problem.operator import WithPrecondition

LOGGER = logging.getLogger(__name__)

Step = namedtuple('Step', ['operator', 'begin', 'end'])
Decomposition = namedtuple('Decomposition', ['method', 'substeps'])
CausalLink = namedtuple('CausalLink', ['link', 'support'])
OpenLink = namedtuple('OpenLink', ['step', 'literal', 'value'])
Threat = namedtuple('Threat', ['step', 'link'])


class HierarchicalPartialPlan:
    def __init__(self, problem: Problem,
                 init: bool = False,
                 goal: bool = False,
                 poset_inc_impl: bool = True):
        self.__problem = problem
        self.__steps = dict()
        self.__tasks = set()
        # Plan links
        self.__poset = (IncrementalPoset() if poset_inc_impl else Poset())
        self.__hierarchy = dict()
        self.__causal_links = set()
        # Plan flaws
        self.__open_links = set()
        self.__threats = set()
        self.__abstract_flaws = set()
        # Untested flaws
        self.__freezed_flaws = False
        self.__pending_open_links = set()
        self.__pending_threats = SortedKeyList(key=lambda t: len(t[1]))
        self.__pending_abstract_flaws = set()
        # Goal step default
        self.__goal_step = None
        self.__goal = None
        # Init state
        if init:
            self.__initial_step = 0
            self.__build_init()
        else:
            self.__initial_step = 1
            self.__init = None
        # Prepare heuristic computation
        self.__h_tdg = self.__problem.tdg.heuristic
        self.__h_add = self.__problem.h_add.heuristic
        # Goal
        if goal:
            self.__build_goal()

    def __add_step(self, op: str, atomic: bool, **kwargs) -> int:
        index = len(self.__steps) + self.__initial_step
        if atomic:
            step = Step(op, index, index)
            self.__poset.add(index, operator=op, **kwargs)
        else:
            step = Step(op, index, -index)
            self.__poset.add(index, operator=op, **kwargs)
            self.__poset.add(-index, operator=op, **kwargs)
            self.__poset.add_relation(index, -index)
        if (self.__init is not None) and (index != 0):
            self.__poset.add_relation(0, index)
        if (self.__goal_step is not None):
            self.__poset.add_relation(step.end, self.__goal_step)
        self.__steps[index] = step
        LOGGER.debug("add step %d %s", index, step)
        return index

    def __build_init(self):
        _, pddl_problem = self.__problem.pddl
        add_eff = pddl.AndFormula(pddl_problem.init)
        del_eff = pddl.AndFormula([pddl.AtomicFormula(pred, args) for (pred, args) in
                                   map(Literals.lit_to_predicate, self.__problem.init_falses)])
        self.__init = GroundedAction(pddl.Action('__init', effect=pddl.AndFormula([add_eff, pddl.NotFormula(del_eff)])),
                                     None, set(), set(), objects=self.__problem.objects)
        __init_step = self.add_action(self.__init)
        LOGGER.debug("Added INIT step %d", __init_step)

    def __build_goal(self):
        _, pddl_problem = self.__problem.pddl
        if pddl_problem.goal:
            self.__goal = GroundedAction(pddl.Action('__goal', precondition=pddl_problem.goal),
                                         {}, set(), set(), self.__problem.objects)
            self.__goal_step = self.add_action(self.__goal)
            LOGGER.debug("Added GOAL step %d", self.__goal_step)

    def __copy__(self):
        new_plan = HierarchicalPartialPlan(self.__problem, False)
        new_plan.__steps = copy(self.__steps)
        new_plan.__tasks = copy(self.__tasks)
        new_plan.__hierarchy = copy(self.__hierarchy)
        new_plan.__causal_links = copy(self.__causal_links)
        new_plan.__open_links = copy(self.__open_links)
        new_plan.__threats = copy(self.__threats)
        new_plan.__abstract_flaws = copy(self.__abstract_flaws)
        new_plan.__init = self.__init
        new_plan.__goal = self.__goal
        new_plan.__goal_step = self.__goal_step
        new_plan.__poset = copy(self.__poset)
        return new_plan

    def __relevant_nodes(self):
        relevant_nodes = dict()
        linked_steps = set(cl.link.step for cl in self.__causal_links)
        for index, step in self.__steps.items():
            if index in self.__tasks:
                if index in self.__abstract_flaws:
                    relevant_nodes[step.begin] = step.operator
                    relevant_nodes[step.end] = step.operator
                if index in linked_steps:
                    relevant_nodes[step.begin] = step.operator
            else:
                relevant_nodes[step.begin] = step.operator
                relevant_nodes[step.end] = step.operator
        return relevant_nodes

    def __eq__(self, plan: 'HierarchicalPartialPlan') -> bool:
        if self.empty and plan.empty:
            return True

        s1 = [self.__steps[k].operator for k in self.__steps if k not in self.__tasks]
        s2 = [plan.__steps[k].operator for k in plan.__steps if k not in plan.__tasks]
        if s1 != s2:
            return False

        if len(self.__causal_links) != len(plan.__causal_links):
            return False
        if len(self.__abstract_flaws) != len(plan.__abstract_flaws):
            return False
        if len(self.__threats) != len(plan.__threats):
            return False
        if len(self.__open_links) != len(plan.__open_links):
            return False

        cl1 = [l.link.literal for l in self.__causal_links]
        cl2 = [l.link.literal for l in plan.__causal_links]
        if cl1 != cl2:
            return False

        ol1 = [l.literal for l in self.__open_links]
        ol2 = [l.literal for l in plan.__open_links]
        if ol1 != ol2:
            return False

        return self.__poset.sameas(plan.__poset, self.__relevant_nodes(), plan.__relevant_nodes())

    @property
    def f(self) -> int:
        """
        Heuristics calculated from h_add,
        the sum of the cost of each open link:
        f(P) = g(P) + h(P)
        g(P) = \Sum_s\inP {cost(a) if s is action ; m if s is abstract with m methods}
        NB: We do not consider action reuse (actually)
        :return: heuristic value of the plan
        """
        g = sum(self.__problem.get_action(a.operator).cost
                for a in self.__steps.values()
                if self.__problem.has_action(a.operator))

        #return g + self.hadd + self.htdg_min_hadd
        # return g + self.hadd + self.htdg_max_hadd
        # return g + self.hestim
        return g + self.hadd + self.htdg

    @property
    def hadd(self):
        """
        h(P) = \Sum_l\inOL(P) hadd(l)
        """
        h = sum(self.__h_add(link.literal) for link in self.__open_links)
        return h

    @property
    def htdg(self):
        """
        h(P) = \Sum astract decompositions
        """
        h = sum(self.__h_tdg(self.__steps[t].operator).tdg for t in self.__abstract_flaws)
        return h

    @property
    def htdg_full(self):
        """
        h(P) = g + \Sum_l\inOL(P) hadd(l)
        """
        g = sum(self.__problem.get_action(a.operator).cost
                for a in self.__steps.values()
                if self.__problem.has_action(a.operator))
        h = sum(self.__h_tdg(self.__steps[t].operator).tdg for t in self.__abstract_flaws)
        return g + h

    @property
    def htdg_min_hadd(self):
        h = sum(self.__h_tdg(self.__steps[t].operator).min_hadd for t in self.__abstract_flaws)
        return h

    @property
    def htdg_max_hadd(self):
        h = sum(self.__h_tdg(self.__steps[t].operator).max_hadd for t in self.__abstract_flaws)
        return h

    @property
    def htdg_max_hadd_deep(self):
        h = self.htdg_max_hadd +  self.hadd
        return h

    @property
    def htdg_min_hadd_deep(self):
        h = self.htdg_min_hadd +  self.hadd
        return h

    @property
    def h_avg(self):
        """
        Average of the actions in the plan
        :return: average value of hadd
        """
        num_actions = len([a for a in self.__steps.values()
                           if self.__problem.has_action(a.operator)])
        if num_actions:
            return self.hadd / num_actions
        # todo: deal with inf elements: i.e. do not delete them. see zenotravel
        return math.inf

    @property
    def hestim(self):
        h = self.h_avg * self.htdg + self.hadd
        return h

    @property
    def empty(self) -> bool:
        return (not self.__steps
                or not self.__poset.nodes
                )

    @property
    def poset(self) -> Poset:
        return self.__poset

    @property
    def tasks(self):
        return self.__tasks

    def get_decomposition(self, task: int):
        return self.__hierarchy[task]

    def get_step(self, step: int) -> Any:
        """Get step from index."""
        try:
            return self.__steps[step]
        except KeyError:
            return None

    def remove_step(self, index: int):
        LOGGER.debug("removing step %d", index)
        try:
            LOGGER.debug("- %s", self.__steps[index].operator)
            step = self.__steps[index]
        except KeyError:
            return
        self.__poset.remove(step.begin)
        self.__poset.remove(step.end)
        if index in self.__tasks:
            self.__tasks.remove(index)
        if index in self.__hierarchy:
            for substep in self.__hierarchy[index].substeps:
                try:
                    self.remove_step(substep)
                except KeyError:
                    pass
            del self.__hierarchy[index]
        del self.__steps[index]

    def __add_open_links(self, step: int, operator: WithPrecondition):
        pos, neg = operator.support
        for literal in pos:
            self.__open_links.add(OpenLink(step=step,
                                           literal=literal,
                                           value=True))
        for literal in neg:
            self.__open_links.add(OpenLink(step=step,
                                           literal=literal,
                                           value=False))

    def add_action(self, action: GroundedAction):
        """Add an action in the plan."""
        index = self.__add_step(str(action), atomic=True, color='blue')
        self.__add_open_links(index, action)
        return index

    def add_task(self, task: GroundedTask):
        """Add an abstract task in the plan."""
        index = self.__add_step(str(task), atomic=False)
        self.__tasks.add(index)
        self.__abstract_flaws.add(index)
        return index

    def decompose_step(self, step: int, method: str) -> Iterable[int]:
        """Decompose a hierarchical task already in the plan."""
        if step not in self.__steps:
            LOGGER.error("Step %d is not in the plan", step)
            return False
        if step not in self.__tasks:
            LOGGER.error("Step %d is not a task in the plan", step)
            return False
        task_step = self.__steps[step]
        task = self.__problem.get_task(task_step.operator)
        try:
            method = task.get_method(method)
        except KeyError:
            LOGGER.error("Task %s has no method %s", task, method)
            LOGGER.error("Task %s methods: %s", task, task.methods)
            return False
        return self.__decompose_method(task_step, method)

    def __decompose_method(self, step: Step, method: GroundedMethod) -> Iterable[int]:
        htn = method.task_network
        substeps = dict()
        actions = list()
        for node in htn.nodes:
            subtask_name = method.subtask(node)
            if self.__problem.has_task(subtask_name):
                subtask = self.__problem.get_task(subtask_name)
                substeps[node] = self.__steps[self.add_task(subtask)]
                LOGGER.debug("Add step %d %s", substeps[node].begin, subtask)
            elif self.__problem.has_action(subtask_name):
                subtask = self.__problem.get_action(subtask_name)
                a = self.add_action(subtask)
                substeps[node] = self.__steps[a]
                actions.append(a)
            self.__poset.add_relation(step.begin, substeps[node].begin)
            self.__poset.add_relation(substeps[node].end, step.end)
            LOGGER.debug("Adding substep %s", substeps[node])
        self.__hierarchy[step.begin] = Decomposition(method.name,
                                                     frozenset(s.begin for s in substeps.values()))
        self.__abstract_flaws.discard(step.begin)
        for (u, v, rel) in htn.edges(data="relation", default='<'):
            step_u = substeps[u]
            step_v = substeps[v]
            self.__poset.add_relation(step_u.end, step_v.begin, relation=rel)
        # if method has preconditions, add open links
        self.__add_open_links(step.begin, method)
        # add threats
        for a in actions:
            if not self.__update_threats_on_action(a):
                return None
        # return sorted substeps
        subgraph = list(s.begin for s in substeps.values()) + list(s.end for s in substeps.values())
        return filter(lambda x: x > 0, self.__poset.topological_sort(subgraph))

    def add_causal_link(self, link: CausalLink) -> bool:
        support = self.__steps[link.support]
        link_step = self.__steps[link.link.step]
        self.__causal_links.add(link)
        pred = Literals.lit_to_predicate(link.link.literal)
        LOGGER.debug("plan %s: add causal link %s", id(self), link)
        dag = self.__poset.add_relation(support.end,
                                        link_step.begin,
                                        relation=pred)
        if dag:
            self.__open_links.discard(link.link)
            if not self.__update_threats_on_causal_link(link):
                return False
        return dag

    def __is_open_link_resolvable(self, link: OpenLink) -> bool:
        # self.__poset.write_dot("open-link-resolvable.dot")
        tdg = self.__problem.tdg
        lit = link.literal
        value = link.value
        for index, step in self.__steps.items():
            if index == link.step: continue
            if not self.__poset.is_less_than(link.step, index):
                if index in self.__abstract_flaws:
                    adds, dels = tdg.effect(step.operator)
                    if (value and lit in adds) or ((not value) and lit in dels):
                        return True
        return False

    def __update_threats_on_causal_link(self, link: CausalLink):
        lit = link.link.literal
        value = link.link.value
        support = self.__steps[link.support]
        link_step = self.__steps[link.link.step]
        for index, step in self.__steps.items():
            if '__init' in step.operator: continue
            if self.__problem.has_task(step.operator): continue
            if step.begin == self.__goal_step: continue
            if index == link.support or index == link.link.step: continue

            action = self.__problem.get_action(step.operator)
            adds, dels = action.effect
            if (value and lit in dels) or ((not value) and lit in adds):
                if self.__poset.is_less_than(step.end, support.end):
                    continue
                if self.__poset.is_less_than(link_step.begin, step.begin):
                    continue
                if self.__poset.is_less_than(support.end, step.end) and self.__poset.is_less_than(step.begin,
                                                                                                  link_step.begin):
                    LOGGER.debug("action %s definitely threatens link %s", index, link)
                    return False
                # Else: step can be simultaneous
                threat = Threat(step=index, link=link)
                self.__threats.add(threat)
                LOGGER.debug("adding threats: %s", threat)
        return True

    def __update_threats_on_action(self, index: int):
        if index == 0: return
        if index == self.__goal_step: return
        step = self.__steps[index]
        action = self.__problem.get_action(step.operator)
        adds, dels = action.effect
        for cl in self.__causal_links:
            lit = cl.link.literal
            value = cl.link.value
            support = self.__steps[cl.support]
            link_step = self.__steps[cl.link.step]
            if (value and lit in dels) or ((not value) and lit in adds):
                if self.__poset.is_less_than(step.end, support.end):
                    continue
                if self.__poset.is_less_than(link_step.begin, step.begin):
                    continue
                if self.__poset.is_less_than(support.end, step.end) and self.__poset.is_less_than(step.begin,
                                                                                                  link_step.begin):
                    LOGGER.debug(
                        "action %s definitely threatens link %s", index, cl)
                    return False
                # Else: step can be simultaneous
                threat = Threat(step=index, link=cl)
                self.__threats.add(threat)
                LOGGER.debug("adding threats: %s", threat)
        return True

    def get_best_flaw(self):
        if not self.__freezed_flaws:
            self.compute_flaw_resolvers()
        if bool(self.__pending_threats):
            # h(threads) = \Sum_t resolvers(p)
            flaw, _ = self.__pending_threats.pop(0)
        elif bool(self.__pending_open_links):
            flaw, _ = self.__pending_open_links.pop(0)
        elif bool(self.__pending_abstract_flaws):
            flaw = self.__pending_abstract_flaws.pop(0)
        else:
            flaw = None
        if flaw is not None:
            LOGGER.debug("returning best flaw {}".format(flaw))
        return flaw

    @property
    def abstract_flaws(self) -> Set[int]:
        """Return the set of Hierarchy Flaws in the plan."""
        return self.__abstract_flaws

    @property
    def open_links(self) -> Set[OpenLink]:
        """Return the set of Open Causal Links in the plan."""
        return self.__open_links

    @property
    def threats(self) -> Set[Threat]:
        """Return the set of Threats on Causal Links in the plan."""
        return self.__threats

    @property
    def pending_abstract_flaws(self) -> Set[int]:
        """Return the set of Hierarchy Flaws in the plan."""
        return self.__pending_abstract_flaws

    @property
    def pending_open_links(self) -> Set[OpenLink]:
        """Return the set of Open Causal Links in the plan."""
        return self.__pending_open_links

    @property
    def pending_threats(self) -> SortedKeyList:
        """Return the set of Threats on Causal Links in the plan."""
        return self.__pending_threats

    @property
    def has_flaws(self) -> bool:
        return bool(self.__threats) or bool(self.__open_links) or bool(self.__abstract_flaws)

    @property
    def has_pending_flaws(self) -> bool:
        if not self.__freezed_flaws:
            self.compute_flaw_resolvers()
        return bool(self.__pending_threats) or bool(self.__pending_open_links) or bool(self.__pending_abstract_flaws)

    def compute_flaw_resolvers(self) -> bool:
        if not self.__freezed_flaws:
            self.__resolvers = dict()
            # Abstract Flaws are sorted chronologically
            self.__pending_abstract_flaws = list(self.__poset.topological_sort(nodes=self.__abstract_flaws))
            for flaw in self.__abstract_flaws:
                resolvers = list(self.__resolve_abstract_flaw(flaw))
                if len(resolvers) == 0:
                    LOGGER.debug("AbstractFlaw %s cannot be resolved", flaw)
                    return False
                self.__resolvers[flaw] = resolvers
            # Threats are sorted by number of resolvers
            self.__pending_threats = SortedKeyList(key=itemgetter(1))
            for flaw in self.__threats:
                resolvers = list(self.__resolve_threat(flaw))
                self.__resolvers[flaw] = resolvers
                if len(resolvers) == 0:
                    LOGGER.debug("Threat %s cannot be resolved", flaw)
                    return False
                self.__pending_threats.add((flaw, len(self.__resolvers[flaw])))
            # Open Links
            ol_steps_ordered = list(self.__poset.topological_sort(
                nodes=[link.step for link in self.__open_links]))
            self.__pending_open_links = SortedKeyList(key=itemgetter(1))
            for flaw in self.__open_links:
                resolvers = list(self.__resolve_open_link(flaw))
                self.__resolvers[flaw] = resolvers
                if len(resolvers) > 0:
                    # sort open links chronologically:
                    self.__pending_open_links.add((flaw, ol_steps_ordered.index(flaw.step)))
                    # sort open links by h_add max:
                    # self.__pending_open_links.add((flaw, -self.__h_add(flaw.literal)))
                elif len(self.__abstract_flaws) == 0:
                    LOGGER.debug("OpenLink %s cannot be resolved", flaw)
                    return False
                elif math.isinf(self.__h_add(flaw.literal)) or (not self.__is_open_link_resolvable(flaw)):
                    LOGGER.debug("OpenLink %s could not be resolved ever!", flaw)
                    return False
            self.__freezed_flaws = True
        return True

    def resolvers(self, flaw):
        if not self.__freezed_flaws:
            self.compute_flaw_resolvers()
        return self.__resolvers[flaw]

    def __resolve_abstract_flaw(self, flaw: int) -> Iterator['HierarchicalPartialPlan']:
        if flaw not in self.__abstract_flaws:
            LOGGER.error("Step %d is not an abstract flaw in the plan", flaw)
            return ()
        task_step = self.__steps[flaw]
        task = self.__problem.get_task(task_step.operator)
        LOGGER.debug("resolving abstract flaw %d %s", flaw, task_step.operator)
        if not task.methods:
            LOGGER.debug("- no resolvers")
            return ()
        for method in task.methods:
            plan = copy(self)
            if plan.__decompose_method(task_step, method) is not None:
                LOGGER.debug("- found resolver with method %s", method)
                yield plan

    def __resolve_open_link(self, link: OpenLink) -> Iterator['HierarchicalPartialPlan']:
        if link not in self.__open_links:
            LOGGER.error("Causal Link %s is not an open link in the plan", link)
            LOGGER.debug("Open links: %s", self.__open_links)
            return ()
        link_step = self.__steps[link.step]
        lit = link.literal
        for index, step in self.__steps.items():
            try:
                if '__init' in step.operator:
                    action = self.__init
                else:
                    action = self.__problem.get_action(step.operator)
            except:
                # This step is not an action -- pass
                continue
            if self.__poset.is_less_than(link_step.begin, step.end): continue
            # Get action effects
            adds, dels = action.effect
            if link.value and (lit in adds):
                LOGGER.debug("action %s provides literal %s", action, lit)
                cl = CausalLink(link=link, support=index)
            elif (not link.value) and (lit in dels):
                LOGGER.debug("action %s removes literal %s", action, lit)
                cl = CausalLink(link=link, support=index)
            else:
                cl = None
            if cl:
                plan = copy(self)
                if plan.add_causal_link(cl):
                    yield plan

    def __resolve_threat(self, threat: Threat) -> Iterator['HierarchicalPartialPlan']:
        step = self.__steps[threat.step]
        support = self.__steps[threat.link.support]
        supported = self.__steps[threat.link.link.step]
        if self.__poset.is_less_than(step.end, support.end):
            yield self
            return
        if self.__poset.is_less_than(supported.begin, step.begin):
            yield self
            return
        # Before
        bplan = copy(self)
        if bplan.__poset.add_relation(step.end, support.end):
            bplan.__threats.discard(threat)
            yield bplan
        # After
        aplan = copy(self)
        if aplan.__poset.add_relation(supported.begin, step.begin):
            aplan.__threats.discard(threat)
            yield aplan

    def graphviz_string(self) -> str:
        self.__poset.reduce()
        graph = 'digraph {\n' + '\n'.join(map(lambda x: f"{x[0]} -> {x[1]};", self.__poset.edges))
        for index, step in self.__steps.items():
            if index in self.__tasks and index in self.__hierarchy:
                decomp = self.__hierarchy[index]
                graph += f'\nsubgraph cluster_{index} ' + "{\nstyle = dashed;\n"
                graph += f'label = "{index} / {step.operator} / {decomp.method}";\n'
                graph += "".join(map(lambda x: f"cluster_{x};\n", decomp.substeps))
                graph += f"{step.begin}; {step.end};\n"
                graph += "}\n"
            else:
                graph += f'\nsubgraph cluster_{index} ' + "{\nstyle = solid;\n"
                graph += f'label = "{index} / {step.operator}";\n'
                graph += f"{step.begin}; {step.end};\n"
                graph += "}\n"
        graph += "}"
        return graph

    def write_dot(self, filename):
        networkx.nx_pydot.write_dot(self.__poset.poset, filename)

    def sequential_plan(self):
        """Return a sequential version of the plan."""
        sequence = list(self.__poset.topological_sort())
        LOGGER.debug("top. sort: %s", sequence)
        return [(i, self.__steps[i]) for i in sequence if i > 0]
