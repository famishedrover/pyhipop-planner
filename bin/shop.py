import sys
import os
import argparse
import logging
import time
import itertools
import networkx

import pddl
from hipop.problem.problem import Problem
from hipop.search.shop import SHOP
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
    problem = Problem(pddl_problem, pddl_domain)
    toc = time.process_time()
    LOGGER.warning("building problem duration: %.3f", (toc - tic))

    for node in problem.tdg.nodes:
        try:
            cycles = networkx.find_cycle(problem.tdg, node)
            LOGGER.info("From %s, cycle of length %d", node, len(cycles))
        except networkx.NetworkXNoCycle:
            pass

    stop_profiling(args.trace_malloc, profiler)

    LOGGER.info("Solving problem with SHOP")
    tic = time.process_time()
    shop = SHOP(problem)
    plan = shop.find_plan(problem.init,
                          list(problem.goal_task.sorted_tasks))
    toc = time.process_time()
    LOGGER.warning("SHOP solving duration: %.3f", (toc - tic))

    if plan is None:
        LOGGER.error("No plan found!")
        sys.exit(0)

    from hipop.utils.io import output_ipc2020
    import io
    out_plan = io.StringIO()
    output_ipc2020(plan, out_plan)
    print(out_plan.getvalue())

if __name__ == '__main__':
    main()
