import sys
import unittest
import logging

import pddl

from hipop.problem import Problem

class TestGrounding(unittest.TestCase):

    domain = """(define (domain test-grounding)
        (:types B - A C)
        (:action test-action
         :parameters (?x - A ?y - C)
         )
        )
        """
    problem = """(define (problem test-grounding-pb)
        (:domain test-grounding)
        (:objects a - A b - B c1 c2 - C)
        (:init)
        )
        """

    def test_grounding(self):
        pddl_problem = pddl.parse_problem(self.problem)
        pddl_domain = pddl.parse_domain(self.domain)
        problem = Problem(pddl_problem, pddl_domain)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    unittest.main()
