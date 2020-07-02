import logging
import math
import io
import subprocess
import time
import os
from pathlib import Path
import argparse
import matplotlib.pyplot as plt

import pddl
from hipop.problem.problem import Problem
from hipop.search.shop import SHOP
from hipop.utils.logger import setup_logging
from hipop.utils.io import output_ipc2020_flat, output_ipc2020_hierarchical

LOGGER = logging.getLogger(__name__)

class Statistics:
    def __init__(self, domain, problem, alg):
        self.domain = domain
        self.problem = problem
        self.alg = alg
        self.parsing_time = math.inf
        self.problem_time = math.inf
        self.solving_time = math.inf
        self.verif = False
    def __str__(self):
        return f"{self.domain} {self.problem} {self.alg} {self.parsing_time} {self.problem_time} {self.solving_time} {self.verif}"

def setup():
    setup_logging(level=logging.ERROR)

def process_problem(domain, problem, alg):
    tic = time.process_time()
    LOGGER.info("Parsing PDDL domain %s", domain)
    pddl_domain = pddl.parse_domain(domain, file_stream=True)
    LOGGER.info("Parsing PDDL problem %s", problem)
    pddl_problem = pddl.parse_problem(problem, file_stream=True)
    toc = time.process_time()
    LOGGER.info("parsing duration: %.3f", (toc - tic))

    stats = Statistics(pddl_domain.name, pddl_problem.name, alg)
    stats.parsing_time = (toc - tic)

    tic = time.process_time()
    LOGGER.info("Building HiPOP problem")
    shop_problem = Problem(pddl_problem, pddl_domain,
                      filter_static=True, tdg_filter_useless=True,
                      htn_problem=True)
    toc = time.process_time()
    stats.problem_time = (toc - tic)
    LOGGER.info("building problem duration: %.3f", (toc - tic))

    LOGGER.info("Solving problem with SHOP")
    tic = time.process_time()
    if alg.lower() == 'shop':
        shop = SHOP(shop_problem, no_duplicate_search=True, hierarchical_plan=False)
        output = output_ipc2020_flat
    elif alg.lower() == 'hshop':
        shop = SHOP(shop_problem, no_duplicate_search=True,
                    hierarchical_plan=True, poset_inc_impl=False)
        output = output_ipc2020_hierarchical
    elif alg.lower() == 'hshop-inc':
        shop = SHOP(shop_problem, no_duplicate_search=True,
                    hierarchical_plan=True, poset_inc_impl=True)
        output = output_ipc2020_hierarchical
    plan = shop.find_plan(shop_problem.init, shop_problem.goal_task)
    toc = time.process_time()
    stats.solving_time = (toc - tic)
    LOGGER.info("SHOP solving duration: %.3f", (toc - tic))

    if plan is None:
        LOGGER.error("No plan found!")
        return stats
    #else:
    out_plan = open("plan.plan", "w", encoding="utf-8")
    output(plan, out_plan)
    out_plan.close()

    verificator = subprocess.Popen(["../pandaPIparser/pandaPIparser",
                                    "-verify",
                                    domain,
                                    problem,
                                    "plan.plan"], stdout=subprocess.PIPE)
    verification = verificator.stdout.read().decode(encoding='utf-8')
    stats.verif = verification.count("true")
    return stats

def process_domain(benchmark, bench_root, max_bench):
    root = os.path.join(bench_root, benchmark)
    domain = next(Path(os.path.join(root, 'domains')).rglob('*.?ddl'))
    bench = 1
    shop_data = []
    hshop_data = []
    hshopi_data = []
    problems = []
    for problem in sorted(Path(os.path.join(root, 'problems')).rglob('*.?ddl')):
        problems.append(problem)
        stats = process_problem(domain, problem, 'shop')
        print(stats)
        shop_data.append(stats)
        try:
            stats = process_problem(domain, problem, 'hshop')
        except:
            stats = Statistics(domain.name, problem.name, 'hshop')
        print(stats)
        hshop_data.append(stats)
        stats = process_problem(domain, problem, 'hshop-inc')
        print(stats)
        hshopi_data.append(stats)
        bench += 1
        if bench > max_bench:
            break
    return problems, shop_data, hshop_data, hshopi_data

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SHOP planner")
    parser.add_argument("benchmark", help="Benchmark name", type=str)
    parser.add_argument("-N", "--nb-problems", default=math.inf,
                        help="Number of problems to solve", type=int)
    parser.add_argument("-p", "--benchmark-prefix", help="Prefix path to benchmarks",
                        type=str)
    parser.add_argument("-P", "--plot", help="Plot results", action="store_true")
    args = parser.parse_args()

    setup()
    if args.benchmark_prefix:
        bench_root = args.benchmark_prefix
    else:
        bench_root = os.path.join('..', 'benchmarks', 'ipc2020-hierarchical', 'HDDL-total')
    problems, shop_data, hshop_data, hshopi_data = process_domain(args.benchmark, bench_root, args.nb_problems)
    if args.plot:
        plt.plot(range(len(problems)), [x.solving_time for x in shop_data], 'r-x', label="SHOP")
        plt.plot(range(len(problems)), [x.solving_time for x in hshop_data], 'b-o', label="H-SHOP")
        plt.plot(range(len(problems)), [x.solving_time for x in hshopi_data], 'g-s', label="H-SHOP-INC")
        plt.xticks([x for x in range(len(problems))], [f"{x+1}" for x in range(len(problems))])
        plt.xlabel("problem")
        plt.ylabel("solving time (s)")
        plt.title(args.benchmark)
        plt.legend()
        plt.show()
