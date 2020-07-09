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
        (:task task )
        (:method m1
         :task (task)
         :ordered-subtasks (noop)
        )
        (:method m2
         :task (task)
         :ordered-subtasks (noop)
        )
        (:action noop
         :precondition ()
         :effect ()
        )
        )
        """
    problem = """(define (problem test-flaws-pb)
        (:domain test-flaws)
    	(:htn
		 :subtasks (and (task0 (task)))
    	)
        (:init )
        )
        """

    def test_abstract_flaw(self):
        pddl_problem = pddl.parse_problem(self.problem)
        pddl_domain = pddl.parse_domain(self.domain)
        problem = Problem(pddl_problem, pddl_domain, htn_problem=False, tdg_filter_useless=False)
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

def main():
    setup_logging(logging.DEBUG)
    unittest.main()


if __name__ == '__main__':
    main()
