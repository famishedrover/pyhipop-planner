import sys
import unittest
import logging
from copy import deepcopy, copy
import pddl

from hipop.utils.poset import Poset, IncrementalPoset
from hipop.utils.logger import setup_logging

class TestPoset(unittest.TestCase):

    def test_poset(self):
        poset = Poset()
        poset.add('A')
        poset.add_relation('A', ['B', 'C'])
        poset.add('B')
        poset.add_relation('B', 'D')
        poset.add('C')
        poset.add_relation('C', ['D', 'E'])
        self.assertTrue(poset.is_less_than('A', 'B'))
        self.assertTrue(poset.is_less_than('A', 'C'))
        self.assertTrue(poset.is_less_than('A', 'D'))
        self.assertFalse(poset.has_top())
        self.assertTrue(poset.has_bottom())
        self.assertEqual(poset.bottom(), 'A')
        self.assertIn('D', poset.maximal_elements())
        logging.getLogger(__name__).info('%s', poset.graphviz_string(reduce=True))
        logging.getLogger(__name__).info('topo.-sort: %s', "->".join(poset.topological_sort()))

    def test_poset_inc(self):
        poset = IncrementalPoset()
        poset.add('A')
        poset.add('B')
        poset.add('C')
        poset.add('D')
        poset.add('E')
        poset.add_relation('A', ['B', 'C'])
        poset.add_relation('B', 'D')
        poset.add_relation('C', ['D', 'E'])
        self.assertTrue(poset.is_less_than('A', 'B'))
        self.assertTrue(poset.is_less_than('A', 'C'))
        self.assertTrue(poset.is_less_than('A', 'D'))
        self.assertFalse(poset.has_top())
        self.assertTrue(poset.has_bottom())
        self.assertEqual(poset.bottom(), 'A')
        self.assertIn('D', poset.maximal_elements())
        logging.getLogger(__name__).info('%s', poset.graphviz_string(reduce=True))
        logging.getLogger(__name__).info('topo.-sort: %s', "->".join(poset.topological_sort()))

    def test_copy(self):
        poset = IncrementalPoset()
        poset.add('A')
        poset.add('B')
        poset.add('C')
        poset.add('D')
        poset.add('E')
        poset.add_relation('A', ['B', 'C'])
        poset.add_relation('B', 'D')
        poset.add_relation('C', ['D', 'E'])
        poset_copy = copy(poset)
        self.assertFalse(poset_copy.add_relation('E', 'A'))
        self.assertTrue(poset.is_less_than('A', 'B'))
        self.assertTrue(poset.is_less_than('A', 'C'))
        self.assertTrue(poset.is_less_than('A', 'D'))
        self.assertFalse(poset.has_top())
        self.assertTrue(poset.has_bottom())
        self.assertEqual(poset.bottom(), 'A')
        self.assertIn('D', poset.maximal_elements())
        logging.getLogger(__name__).info(
            '%s', poset_copy.graphviz_string(reduce=True))
        logging.getLogger(__name__).info('topo.-sort: %s',
                                         "->".join(poset.topological_sort()))

def main():
    setup_logging(logging.DEBUG)
    unittest.main()


if __name__ == '__main__':
    main()
