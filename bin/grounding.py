import sys
import os
import argparse
import logging
import time
import itertools

import pddl
from hipop.problem.problem import Problem
from hipop.utils.profiling import start_profiling, stop_profiling
from hipop.utils.logger import setup_logging

LOGGER = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="SHOP planner")
    parser.add_argument("domain", help="PDDL domain file", type=str)
    parser.add_argument("problem", help="PDDL problem file", type=str)
    parser.add_argument("-d", "--debug", help="Activate debug logs",
                        action='store_const', dest="loglevel",
                        const=logging.DEBUG, default=logging.WARNING)
    parser.add_argument("-v", "--verbose", help="Activate verbose logs",
                        action='store_const', dest="loglevel",
                        const=logging.INFO, default=logging.WARNING)
    parser.add_argument("--trace-malloc", help="Activate tracemalloc",
                        action='store_true')
    parser.add_argument("--profile", help="Activate profiling",
                        action='store_true')
    #parser.add_argument("--tdg", help="Activate TDG-based grounding",
    #                    action='store_true')
    args = parser.parse_args()

    setup_logging(level=args.loglevel)

    tic = time.process_time()
    LOGGER.info("Parsing PDDL domain %s", args.domain)
    pddl_domain = pddl.parse_domain(args.domain, file_stream=True)
    LOGGER.info("Parsing PDDL problem %s", args.problem)
    pddl_problem = pddl.parse_problem(args.problem, file_stream=True)
    toc = time.process_time()
    LOGGER.warning("parsing duration: %.3f", (toc - tic))

    profiler = start_profiling(args.trace_malloc, args.profile)

    tic = time.process_time()
    LOGGER.info("Building HiPOP problem")
    problem = Problem(pddl_problem, pddl_domain, True)
    toc = time.process_time()
    LOGGER.warning("building problem duration: %.3f", (toc - tic))

    '''
    import networkx.drawing.nx_pydot
    from hipop.problem.operator import GroundedTask
    tdg = problem.tdg
    networkx.drawing.nx_pydot.write_dot(tdg, "problem-tdg.dot")
    LOGGER.info("TDG size: %d (%d)", tdg.number_of_nodes(),
                (2 + len(problem.actions) + len(problem.tasks) +
                 sum(1 for task in problem.tasks for _ in task.methods)))
    for node in list(problem.actions) + list(problem.tasks):
        is_useless = not networkx.has_path(tdg, '__top', str(node))
        if is_useless:
            LOGGER.warning("TDG: node %s is useless", node)
            tdg.remove_node(str(node))
            if isinstance(node, GroundedTask):
                for node in node.methods:
                    tdg.remove_node(str(node))
    LOGGER.info("TDG size: %d", tdg.number_of_nodes())
    networkx.drawing.nx_pydot.write_dot(tdg, "problem-tdg.dot")
    for node in tdg.nodes:
        try:
            cycles = networkx.find_cycle(tdg, node)
            LOGGER.info("From %s, cycle of length %d", node, len(cycles))
        except networkx.NetworkXNoCycle:
            pass
    LOGGER.info("Weakly Connected Components: %d",
                sum(1 for _ in networkx.weakly_connected_components(tdg)))
    LOGGER.info("Strongly Connected Components: %d",
                sum(1 for _ in networkx.strongly_connected_components(tdg)))

    stop_profiling(args.trace_malloc, profiler)

    '''

if __name__ == '__main__':
    main()
