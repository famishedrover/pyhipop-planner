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

    def test_same(self):
        p = IncrementalPoset()
        for i in range(6):
            p.add(i)
        p.add_relation(0, [1, 2])
        p.add_relation(2, [3, 4])
        p.add_relation(4, 5)
        q = IncrementalPoset()
        q.add(10)
        q.add(47)
        q.add_relation(10, 47)
        self.assertTrue(p.sameas(q, {0: 'A', 5: 'B'}, {10: 'A', 47: 'B'}))

def main():
    setup_logging(logging.DEBUG)
    unittest.main()


if __name__ == '__main__':
    main()
