from typing import Union, Any, Iterator, Optional, Iterable, Set, List, Tuple
from collections import defaultdict
from copy import deepcopy, copy
import math
import logging
import networkx
from operator import itemgetter, attrgetter
from sortedcontainers import SortedKeyList

import pddl
from .flaws import AbstractFlaw, Threat, OpenLink, FlawUnresolvable
from .step import Step
from .links import CausalLink, Decomposition
from .poset import Poset
from ..grounding.atoms import Atoms
from ..grounding.problem import Problem
from ..grounding.operator import GroundedAction, GroundedTask, WithPrecondition, WithEffect

LOGGER = logging.getLogger(__name__)


class HierarchicalPartialPlan:
    def __init__(self, problem: Problem,
                 init: bool = False,
                 goal: bool = False):

        self.__problem = problem
        self.__steps = dict()
        self.__tasks = set()
        self.__methods = set()
        # Plan links
        self.__poset = Poset()
        self.__hierarchy = dict()
        self.__causal_links = list()
        # Plan flaws
        self.__open_links = list()
        self.__threats = list()
        self.__abstract_flaws = list()
        # Goal step default
        self.__goal_step = None
        self.__goal = None
        # Helpers for __eq__ testing
        self.__task_method_decompsition = defaultdict(set)
        self.__operators_atoms_in_causal_links = set()
        # Init state
        self.__init = None
        self.__step_counter = 1
        if init:
            self.__step_counter = 0
            self.__build_init()

        # TODO: Build Goal Action

    #------------- STEPS, ACTIONS, and TAKS ------------------#

    def __build_init(self):
        trues, falses = self.__problem.init
        add_eff = pddl.AndFormula([pddl.AtomicFormula(pred, args) for (pred, args) in
                                   map(Atoms.atom_to_predicate, trues)])
        del_eff = pddl.AndFormula([pddl.AtomicFormula(pred, args) for (pred, args) in
                                   map(Atoms.atom_to_predicate, falses)])
        self.__init_step = self.__add_step('__init', atomic=True, color='grey', link_to_init=False)
        self.__init = GroundedAction(pddl.Action('__init', effect=pddl.AndFormula([add_eff, pddl.NotFormula(del_eff)])),
                                     dict(), literals=self.__problem.literals,
                                     objects=self.__problem.objects,
                                     remove_contradictory_effects=False)
        LOGGER.debug("Added INIT step %d", self.__init_step)

    def __add_step(self, op: str, atomic: bool, link_to_init: bool, **kwargs) -> int:
        index = self.__step_counter
        if atomic:
            step = Step(operator=op, start=index, end=index)
            self.__poset.add(index, operator=op, **kwargs)
        else:
            step = Step(operator=op, start=index, end=-index)
            self.__poset.add(index, operator=op, **kwargs)
            self.__poset.add(-index, operator=op, **kwargs)
            self.__poset.add_relation(index, -index)
        if link_to_init and (self.__init is not None):
            self.__poset.add_relation(self.__init_step, index)
        #if (self.__goal_step is not None):
        #    self.__poset.add_relation(step.end, self.__goal_step)
        self.__steps[index] = step
        self.__step_counter += 1
        return index

    def add_action(self, action: GroundedAction, link_to_init: bool = True) -> int:
        """Add an action in the plan."""
        index = self.__add_step(str(action), atomic=True, 
                    link_to_init=link_to_init, color='blue')
        self.__add_open_links(index, action)
        return index

    def add_task(self, task: GroundedTask, link_to_init: bool = True) -> int:
        """Add an abstract task in the plan."""
        index = self.__add_step(str(task), atomic=False, link_to_init=link_to_init)
        self.__tasks.add(index)
        self.__abstract_flaws.append(AbstractFlaw(index, str(task)))
        return index

    def get_decomposition(self, task: int) -> Decomposition:
        return self.__hierarchy[task]

    #------------- FLAWS ------------------#

    def has_flaws(self) -> bool:
        return bool(self.__threats) or bool(self.__open_links) or bool(self.__abstract_flaws)

    @property
    def flaws(self) -> Tuple[Set[Threat], Set[OpenLink], Set[AbstractFlaw]]:
        return self.__threats, self.__open_links, self.__abstract_flaws

    #------------- ABSTRACT FLAWS ------------------#

    @property
    def abstract_flaws(self) -> Set[AbstractFlaw]:
        return self.__abstract_flaws

    def __abstract_flaw_resolvers(self, flaw: AbstractFlaw) -> List[Decomposition]:
        if flaw not in self.__abstract_flaws:
            LOGGER.error("Abstract flaw %s is not in the plan flaws", flaw)
            return []
        LOGGER.debug("compute resolvers for abstract flaw %s", flaw)
        methods = list(self.__problem.tdg.successors(flaw.task))
        return [Decomposition(flaw, m) for m in methods]

    def abstract_flaw_resolvers(self, flaw: AbstractFlaw) -> Iterator['HierarchicalPartialPlan']:
        modifications = self.__abstract_flaw_resolvers(flaw)
        for m in modifications:
            new_plan = self.copy()
            new_plan.__abstract_flaws.remove(flaw)
            method = self.__problem.method(m.method)
            htn = method.task_network

            substeps = dict()
            actions = list()

            mindex = new_plan.__add_step(
                m.method, atomic=False, link_to_init=False, shape='rectangle')
            substeps['__init'] = Step(mindex, mindex, m.method)
            substeps['__goal'] = Step(-mindex, -mindex, m.method)

            for node in htn.nodes:
                op = htn.nodes[node]['operator']
                if self.__problem.has_task(op):
                    subtask = self.__problem.task(op)
                    index = new_plan.add_task(subtask, link_to_init=False)
                    substeps[node] = new_plan.__steps[index]
                    LOGGER.debug("Add %d: %s", index, substeps[node])
                elif self.__problem.has_action(op):
                    subtask = self.__problem.action(op)
                    index = new_plan.add_action(subtask, link_to_init=False)
                    substeps[node] = new_plan.__steps[index]
                    LOGGER.debug("Add %d: %s", index, substeps[node])
                    actions.append(index)

            flaw_step = new_plan.__steps[flaw.step]
            new_plan.__poset.add_relation(
                flaw_step.start, substeps['__init'].start)
            new_plan.__poset.add_relation(substeps['__goal'].end, flaw_step.end)

            for (u, v, rel) in htn.edges(data="relation", default='<'):
                step_u = substeps[u]
                step_v = substeps[v]
                new_plan.__poset.add_relation(
                    step_u.end, step_v.start, relation=rel)

            # Update decomposition
            m.substeps = [t.start for t in substeps.values()]
            new_plan.__hierarchy[flaw.step] = m
            # helper for __eq__
            new_plan.__task_method_decompsition[flaw.task].add(m.method)

            # if method has preconditions, add open links
            new_plan.__add_open_links(mindex, method)

            # Update threats
            try:
                for a in actions:
                    new_plan.__threats += new_plan.__threats_on_action(a)
            except FlawUnresolvable:
                # a new threat has no possible resolvers
                continue

            yield new_plan

    #------------- OPEN LINKS ------------------#

    def __add_open_links(self, step: int, operator: WithPrecondition):
        pos, neg = operator.support
        for atom in pos:
            self.__open_links.append(OpenLink(step=step,
                                           atom=atom,
                                           value=True))
        for atom in neg:
            self.__open_links.append(OpenLink(step=step,
                                           atom=atom,
                                           value=False))

    @property
    def open_links(self) -> Set[OpenLink]:
        return self.__open_links

    def is_ol_resolvable(self, ol) -> bool:
        return (self.has_open_link_task_resolvers(ol) 
                or
                self.has_ol_direct_resolvers(ol))

    def has_ol_direct_resolvers(self, ol) -> bool:
        return bool(self.__open_link_resolvers(ol))

    def __can_resolve_open_link(self, step: Step, effects: Tuple[Set[int], Set[int]], ol: OpenLink) -> bool:
        ol_step = self.__steps[ol.step]
        if self.__poset.is_less_than(ol_step.start, step.end):
            # Step after link: cannot support the open link
            return False
        adds, dels = effects
        if (ol and ol.atom in adds) or ((not ol) and ol.atom in dels):
            # Literals are ok
            return True
        return False

    def __open_link_resolvers(self, link: OpenLink) -> List[CausalLink]:
        if link not in self.__open_links:
            LOGGER.error("%s is not an open link of the plan", link)
            return []
        resolvers = []
        for index, step in self.__steps.items():
            if self.__problem.has_action(step.operator):
                action = self.__problem.action(step.operator)
            elif '__init' in step.operator:
                action = self.__init
            else:
                # This step is not an action -- continue loop
                continue
            # Get action effects
            if self.__can_resolve_open_link(step, action.effect, link):
                cl = CausalLink(open_link=link, support=index)
                resolvers.append(cl)
        return resolvers

    def open_link_resolvers(self, link: OpenLink) -> Iterator['HierarchicalPartialPlan']:
        modifications = self.__open_link_resolvers(link)
        for cl in modifications:
            new_plan = self.copy()
            new_plan.__causal_links.append(cl)
            new_plan.__open_links.remove(link)
            # __eq__ helper
            x = (cl.atom, new_plan.__steps[cl.support].operator,
                 new_plan.__steps[cl.supported].operator)
            new_plan.__operators_atoms_in_causal_links.add(x)
            # add relation
            support = self.__steps[cl.support]
            supported = self.__steps[cl.supported]
            pred = Atoms.atom_to_predicate(cl.atom)
            LOGGER.debug("add %s", cl)
            dag = new_plan.__poset.add_relation(support.end,
                                    supported.start,
                                    relation=f"{pred if cl else f'not {pred}'}",
                                    check_poset=True)
            if not dag:
                # This resolver is not consistent
                continue
            # Update threats
            try:
                new_plan.__threats += new_plan.__threats_on_causal_link(cl)
            except FlawUnresolvable:
                # a new threat has no possible resolvers
                continue

            yield new_plan

    def has_open_link_task_resolvers(self, ol: OpenLink) -> bool:
        tdg = self.__problem.tdg
        for flaw in self.__abstract_flaws:
            step = self.__steps[flaw.step]
            effects = tdg.task_effects(flaw.task)
            if self.__can_resolve_open_link(step, effects, ol):
                return True
        return False

    #------------- THREATS ------------------#

    @property
    def threats(self) -> Set[Threat]:
        return self.__threats

    def __is_threatening(self, action: GroundedAction, link: CausalLink) -> bool:
        adds, dels = action.effect
        if link: # literal is positive
            if link.atom in dels: # action deletes the literal
                return True
            mutex = self.__problem.mutex(link.atom)
            if mutex and len(adds & mutex) > 0:
                # action adds a mutex of the literal
                return True
        elif link.atom in adds: # action adds the literal
            return True
        return False

    def __threats_on_action(self, step: int) -> List[Threat]:
        threats = list()
        if step == self.__init_step: return threats
        # if index == self.__goal_step: return
        action_step = self.__steps[step]
        action = self.__problem.action(action_step.operator)
        for cl in self.__causal_links:
            support = self.__steps[cl.support]
            supported = self.__steps[cl.supported]
            if self.__is_threatening(action, cl):
                if self.__poset.is_less_than(action_step.end, support.end):
                    # Action ends before production of literal: no threat
                    continue
                if self.__poset.is_less_than(supported.start, action_step.start):
                    # Action starts after consumption of literal: no threat
                    continue
                if (self.__poset.is_less_than(support.end, action_step.end)
                        and self.__poset.is_less_than(action_step.start, supported.start)):
                    # Action cannot be promoted or demoted: error
                    raise FlawUnresolvable()
                # Otherwise, there is a resolvable threat
                threats.append(Threat(step=step, link=cl))
        return threats

    def __threats_on_causal_link(self, link: CausalLink) -> Set[Threat]:
        support = self.__steps[link.support]
        supported = self.__steps[link.supported]
        threats = list()
        for index, step in self.__steps.items():
            if '__init' in step.operator: continue
            if self.__problem.has_task(step.operator): continue
            if self.__problem.has_method(step.operator): continue
            if index == link.support or index == link.supported: continue
            #if step.begin == self.__goal_step:
            #    continue

            action = self.__problem.action(step.operator)
            if self.__is_threatening(action, link):
                if self.__poset.is_less_than(step.end, support.end):
                    # Action ends before production of literal: no threat
                    continue
                if self.__poset.is_less_than(supported.start, step.start):
                    # Action starts after consumption of literal: no threat
                    continue
                if (self.__poset.is_less_than(support.end, step.end) 
                    and self.__poset.is_less_than(step.start, supported.start)):
                    # Action cannot be promoted or demoted: error
                    raise FlawUnresolvable()
                # Otherwise, there is a resolvable threat
                threats.append(Threat(step=index, link=link))
        return threats

    def threat_resolvers(self, threat: Threat) -> Iterator['HierarchicalPartialPlan']:
        step = self.__steps[threat.step]
        support = self.__steps[threat.link.support]
        supported = self.__steps[threat.link.supported]
        # Before
        new_plan = self.copy()
        if new_plan.__poset.add_relation(step.end, support.end, check_poset=True):
            new_plan.__threats.remove(threat)
            yield new_plan
        # After
        new_plan = self.copy()
        if new_plan.__poset.add_relation(supported.start, step.start, check_poset=True):
            new_plan.__threats.remove(threat)
            yield new_plan

    # ------------- COPY and OUTPUTS ---------- #

    def write_dot(self, filename):
        self.__poset.write_dot(filename)

    def sequential_plan(self) -> List[Tuple[int, Step]]:
        """Return a sequential version of the plan."""
        sequence = list(self.__poset.topological_sort())
        return [(i, self.__steps[i]) for i in sequence if i > 0]

    def copy(self) -> 'HierarchicalPartialPlan':
        new_plan = HierarchicalPartialPlan(self.__problem, False)
        new_plan.__init = self.__init
        new_plan.__init_step = self.__init_step
        new_plan.__step_counter = self.__step_counter
        new_plan.__steps = self.__steps.copy()
        new_plan.__tasks = self.__tasks.copy()
        new_plan.__hierarchy = self.__hierarchy.copy()
        new_plan.__causal_links = self.__causal_links.copy()
        new_plan.__open_links = self.__open_links.copy()
        new_plan.__threats = self.__threats.copy()
        new_plan.__abstract_flaws = self.__abstract_flaws.copy()
        #new_plan.__goal = self.__goal
        #new_plan.__goal_step = self.__goal_step
        new_plan.__poset = self.__poset.copy()
        return new_plan

    def __eq__(self, other: 'HierarchicalPartialPlan') -> bool:
        # First, we test size of attributes
        if len(self.__steps) != len(other.__steps):
            return False
        if len(self.__tasks) != len(other.__tasks):
            return False
        if len(self.__hierarchy) != len(other.__hierarchy):
            return False
        if len(self.__causal_links) != len(other.__causal_links):
            return False
        if len(self.__open_links) != len(other.__open_links):
            return False
        if len(self.__threats) != len(other.__threats):
            return False
        if len(self.__abstract_flaws) != len(other.__abstract_flaws):
            return False
        # Tasks/methods decomposition
        if self.__task_method_decompsition != other.__task_method_decompsition:
            return False
        # Operators/Atoms involved in open links
        if self.__operators_atoms_in_causal_links != other.__operators_atoms_in_causal_links:
            return False
        # Abstract flaws
        abs_flaws = set((f.task for f in self.__abstract_flaws))
        other_abs_flaws = set((f.task for f in other.__abstract_flaws))
        if abs_flaws != other_abs_flaws:
            return False
        # Open links
        ols = set((l.atom, self.__steps[l.step].operator) for l in self.__open_links)
        other_ols = set((l.atom, other.__steps[l.step].operator) for l in other.__open_links)
        if ols != other_ols:
            return False
        # Finally, compare graphs
        isomorphic = (self.__poset == other.__poset)
        return isomorphic

    '''
    def __compute_heuristics(self):
        self.__H['g'] = sum(self.__problem.get_action(a.operator).cost
                            for a in self.__steps.values()
                            if self.__problem.has_action(a.operator))
        self.__H['hadd'] = sum(self.__h_add(link.literal) for link in self.__open_links)
        self.__H['htdg'] = sum(self.__h_tdg(self.__steps[t].operator).tdg for t in self.__abstract_flaws)
        self.__H['tdg_min'] = sum(self.__h_tdg(self.__steps[t].operator).min_hadd for t in self.__abstract_flaws)
        self.__H['tdg_max'] = sum(self.__h_tdg(self.__steps[t].operator).max_hadd for t in self.__abstract_flaws)
        self.__H['hadd-reuse'] = sum(self.__h_add(link.literal) for link in self.__open_links
                                    if not self.__has_direct_resolvers(link, False))
        self.__H['hadd-areuse'] = sum(self.__h_add(link.literal) for link in self.__open_links
                                    if not self.__has_direct_resolvers(link, True))

    def __get_h(self, h):
        if h not in self.__H:
            self.__compute_heuristics()
        return self.__H[h]

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
        return self.__get_h('g') + self.__get_h('hadd') + self.__get_h('htdg')

    @property
    def hadd(self):
        """
        h(P) = \Sum_l\inOL(P) hadd(l)
        """
        if self.__hadd_reuse:
            return self.__get_h('hadd-reuse')
        if self.__hadd_areuse:
            return self.__get_h('hadd-areuse')
        return self.__get_h('hadd')

    @property
    def htdg(self):
        """
        h(P) = \Sum astract decompositions
        """
        return self.__get_h('htdg')

    @property
    def htdg_full(self):
        """
        h(P) = g + \Sum_l\inOL(P) hadd(l)
        """
        return self.__get_h('g') + self.__get_h('htdg')

    @property
    def htdg_min_hadd(self):
        return self.__get_h('tdg_min')

    @property
    def htdg_max_hadd(self):
        return self.__get_h('tdg_max')

    @property
    def htdg_max_hadd_deep(self):
        return self.__get_h('tdg_max') + self.__get_h('hadd')

    @property
    def htdg_min_hadd_deep(self):
        return self.__get_h('tdg_min') + self.__get_h('hadd')

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
                if math.isinf(self.__h_add(flaw.literal)):
                    LOGGER.debug(
                        "OpenLink %s has h_add inf!", flaw)
                    return False
                resolvers = list(self.__resolve_open_link(flaw))
                self.__resolvers[flaw] = resolvers
                if len(resolvers) > 0:
                    if self.__ol_earliest:
                        # sort open links chronologically:
                        self.__pending_open_links.add((flaw, ol_steps_ordered.index(flaw.step)))
                    elif self.__ol_sorted:
                        # sort open links by h_add max:
                        self.__pending_open_links.add((flaw, -self.__h_add(flaw.literal)))
                elif not self.__is_open_link_resolvable(flaw):
                    LOGGER.debug("OpenLink %s could not be resolved ever!", flaw)
                    return False
            self.__freezed_flaws = True
        return True

    '''
