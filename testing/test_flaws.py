import sys
import unittest
import logging

import pddl

from hipop.problem.problem import Problem
from hipop.search.plan import HierarchicalPartialPlan
from hipop.utils.logger import setup_logging

LOGGER = logging.getLogger('test_flaws')

class TestFlaws(unittest.TestCase):

    domain = """(define (domain test-flaws)
        (:predicates (p ?x))
        (:task task )
        (:method m1
         :task (task)
         :ordered-subtasks (noop)
        )
        (:method m2
         :task (task)
         :ordered-subtasks (noop)
        )
        (:action noop)
        (:action posop
         :parameters (?x)
         :precondition (p ?x)
         :effect (not (p ?x))
        )
        (:action negop
         :parameters (?x)
         :precondition (not (p ?x))
         :effect (p ?x)
        )
        )
        """
    problem = """(define (problem test-flaws-pb)
        (:domain test-flaws)
        (:objects a)
    	(:htn
		 :subtasks (and (task0 (task)))
    	)
        (:init
            (p a)
        )
        )
        """

    def test_abstract_flaw(self):
        logging.getLogger().setLevel(logging.INFO)
        pddl_problem = pddl.parse_problem(self.problem)
        pddl_domain = pddl.parse_domain(self.domain)
        problem = Problem(pddl_problem, pddl_domain,
                          filter_static=True,
                          htn_problem=False,
                          tdg_filter_useless=False)
        plan = HierarchicalPartialPlan(problem, init=True)
        step = plan.add_task(problem.get_task('(task )'))
        flaws = plan.abstract_flaws
        LOGGER.info("abstract flaws: %s", flaws)
        self.assertEqual(flaws, {step})
        resolvers = list(plan.resolve_abstract_flaw(step))
        for p in resolvers:
            LOGGER.info("resolver candidate: %s", p)
            LOGGER.debug("new abstract flaws: %s", p.abstract_flaws)
            self.assertNotEqual(plan.abstract_flaws, p.abstract_flaws)
        self.assertNotEqual(resolvers[0].get_decomposition(step),
                            resolvers[1].get_decomposition(step))

    def test_open_links(self):
        logging.getLogger().setLevel(logging.DEBUG)
        pddl_problem = pddl.parse_problem(self.problem)
        pddl_domain = pddl.parse_domain(self.domain)
        problem = Problem(pddl_problem, pddl_domain,
                          filter_static=True,
                          htn_problem=False,
                          tdg_filter_useless=False)
        plan = HierarchicalPartialPlan(problem, init=True)
        step_pos = plan.add_action(problem.get_action('(posop a)'))
        step_neg = plan.add_action(problem.get_action('(negop a)'))
        flaws = list(plan.open_links)
        LOGGER.info("open links: %s", flaws)
        self.assertEqual(len(flaws), 2)
        resolvers = list(plan.resolve_open_link(flaws[0]))
        for p in resolvers:
            p.save(f"resolver-link-{flaws[0].literal}.dot")
            LOGGER.debug("new open links: %s", p.open_links)
            self.assertNotEqual(plan.open_links, p.open_links)
        flaw = list(p.open_links)[0]
        resolvers = list(p.resolve_open_link(flaw))
        self.assertEqual(len(resolvers), 0)

def main():
    setup_logging(logging.DEBUG)
    unittest.main()


if __name__ == '__main__':
    main()
