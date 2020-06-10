import sys
import argparse
import pddl
import logging
import time

from .problem import Problem

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="HiPOP planner")
    parser.add_argument("domain", help="PDDL domain file", type=str)
    parser.add_argument("problem", help="PDDL problem file", type=str)
    parser.add_argument("-d", "--debug", help="Activate debug logs",
                        action='store_const', dest="loglevel",
                        const=logging.DEBUG, default=logging.WARNING)
    parser.add_argument("-v", "--verbose", help="Activate verbose logs",
                        action='store_const', dest="loglevel",
                        const=logging.INFO, default=logging.WARNING)
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stderr,
                        level=args.loglevel,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    tic = time.process_time()
    logging.info(f"Parsing PDDL domain {args.domain}")
    pddl_domain = pddl.parse_domain(args.domain, file_stream=True)
    logging.info(f"Parsing PDDL problem {args.problem}")
    pddl_problem = pddl.parse_problem(args.problem, file_stream=True)
    toc = time.process_time()
    logging.warn(f"parsing duration: {toc-tic}")

    tic = time.process_time()
    logging.info(f"Building HiPOP problem")
    problem = Problem(pddl_problem, pddl_domain)
    toc = time.process_time()
    logging.warn(f"building problem duration: {toc-tic}")
