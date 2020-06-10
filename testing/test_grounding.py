import sys
import unittest
import logging

import pddl

from hipop.problem import Problem


class TestGrounding(unittest.TestCase):

    domain = """(define (domain test-grounding)
        (:types B - A C)
        (:predicates (pred ?x - A))
        (:action test-action
         :parameters (?x - A ?y - C)
         :precondition (and (pred ?y) (not (pred ?x)))
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
        self.assertTrue(problem.action('(test-action a c1)'))


def main():
    logformat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                        format=logformat)
    unittest.main()


if __name__ == '__main__':
    main()
