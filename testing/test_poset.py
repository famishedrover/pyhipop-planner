import sys
import unittest
import logging

import pddl

from hipop.utils.poset import Poset


class TestPoset(unittest.TestCase):

    def test_add(self):
        poset = Poset()
        poset.add('A', ['B', 'C'])
        poset.add('B', ['D'])
        poset.add('C', ['D', 'E'])
        self.assertTrue(poset.is_less_than('A', 'B'))
        self.assertTrue(poset.is_less_than('A', 'C'))
        self.assertTrue(poset.is_less_than('A', 'D'))
        self.assertFalse(poset.has_top())
        self.assertTrue(poset.has_bottom())
        self.assertEqual(poset.bottom(), 'A')
        self.assertIn('D', poset.maximal_elements())
        logging.getLogger(__name__).info('%s', poset.graphviz_string(reduce=True))
        logging.getLogger(__name__).info('topo.-sort: %s', "->".join(poset.topological_sort()))

def main():
    logformat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                        format=logformat)
    unittest.main()


if __name__ == '__main__':
    main()
