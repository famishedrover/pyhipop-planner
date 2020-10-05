import sys
import os
import argparse
import logging
import time
import itertools
import io
import subprocess
from tempfile import NamedTemporaryFile

import pddl
from .grounding.problem import Problem
from .search.greedy import GreedySearch, OpenLinkHeuristic, PlanHeuristic, HaddVariant
from .utils.profiling import start_profiling, stop_profiling
from .utils.logger import setup_logging
from .utils.io import output_ipc2020_hierarchical
from .utils.cli import add_bool_arg, EnumAction

LOGGER = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="pyHiPOP")
    parser.add_argument("domain", help="PDDL domain file", type=str)
    parser.add_argument("problem", help="PDDL problem file", type=str)
    parser.add_argument("-d", "--debug", help="activate debug logs",
                    action='store_const', dest="loglevel",
                    const=logging.DEBUG, default=logging.WARNING)
    parser.add_argument("-v", "--verbose", help="activate verbose logs",
                    action='store_const', dest="loglevel",
                    const=logging.INFO, default=logging.WARNING)
    parser.add_argument("-o", "--output-graph",
                    const='', default=None,
                    action='store', nargs='?',
                    help="generate output graphs")
    parser.add_argument("--trace-malloc", help="activate tracemalloc",
                    action='store_true')
    parser.add_argument("--profile", help="activate profiling",
                    action='store_true')
    parser.add_argument("--panda", help="path to the PANDA plan verifier",
                    type=str)
    add_bool_arg(parser, 'filter-rigid', 'rigid',
                 "use rigid relations to filter groundings", True)
    add_bool_arg(parser, 'filter-relaxed', 'relaxed',
                 "use delete-relaxation to filter groundings", True)
    add_bool_arg(parser, 'htn', 'htn',
                 "use pure HTN decomposition", True)
    add_bool_arg(parser, 'mutex', 'mutex',
                 "compute mutex on (motion) predicates", True)
    add_bool_arg(parser, 'inc-poset', 'incposet',
                 "use incremental poset impl.", False)
    parser.add_argument("--ol", help="heuristic to sort open links",
                        type=OpenLinkHeuristic, default=OpenLinkHeuristic.LIFO,
                        action=EnumAction)
    parser.add_argument("--plan", help="heuristic to sort plans",
                        type=PlanHeuristic, default=PlanHeuristic.DEPTH,
                        action=EnumAction)
    parser.add_argument("--hadd", help="Hadd variant",
                        type=HaddVariant, default=HaddVariant.HADD,
                        action=EnumAction)

    args = parser.parse_args()
    setup_logging(level=args.loglevel, without=['pddl'])

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
    problem = Problem(pddl_problem, pddl_domain, args.output_graph,
                      args.rigid, args.relaxed, args.htn, mutex=args.mutex)
    toc = time.process_time()
    LOGGER.warning("grounding duration: %.3f", (toc - tic))

    stop_profiling(args.trace_malloc, profiler, "profile-grounding.stat")
    profiler = start_profiling(args.trace_malloc, args.profile)

    LOGGER.info("Solving problem")
    tic = time.process_time()
    solver = GreedySearch(problem,
                          ol_heuristic=args.ol,
                          plan_heuristic=args.plan,
                          hadd_variant=args.hadd,
                          inc_poset=args.incposet)
    plan = solver.solve(output_current_plan=args.output_graph)
    toc = time.process_time()
    LOGGER.warning("solving duration: %.3f", (toc - tic))
    stop_profiling(args.trace_malloc, profiler, "profile-solving.stat")

    if plan is None:
        LOGGER.error("No plan found!")
        sys.exit(1)

    out_plan = io.StringIO()
    output_ipc2020_hierarchical(plan, problem, out_plan)
    plan = out_plan.getvalue()
    print(plan)
    out_plan.close()

    if args.panda:
        with NamedTemporaryFile(dir='.', suffix=".plan", delete=False) as tmpfile:
            plan_file = tmpfile.name
            LOGGER.info("writing plan to file %s", plan_file)
            tmpfile.write(plan.encode(encoding='utf-8'))

        cmd = [args.panda,
               "-verify",
               args.domain,
               args.problem,
               plan_file]
        LOGGER.info("verification command: %s", cmd)
        verificator = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        verification = verificator.stdout.read().decode(encoding='utf-8')
        print(verification)

if __name__ == '__main__':
    main()
