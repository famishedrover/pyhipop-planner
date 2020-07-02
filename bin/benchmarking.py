import logging
import math
import io
import subprocess
import time
import os
import threading
from pathlib import Path
import argparse
from collections import defaultdict
import matplotlib.pyplot as plt
from copy import deepcopy

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

class SolveThread(threading.Thread):

    def __init__(self, problem, alg, stats):
        threading.Thread.__init__(self)
        self.problem = problem
        self.alg = alg
        self.stats = stats
        self.stats.alg = alg

    def terminate(self):
        try:
            self.shop.stop()
            self.join()
        except AttributeError:
            pass

    def run(self):
        LOGGER.info("Solving problem with SHOP")
        tic = time.process_time()
        if self.alg.lower() == 'shop':
            self.shop = SHOP(self.problem, no_duplicate_search=True, hierarchical_plan=False)
            output = output_ipc2020_flat
        elif self.alg.lower() == 'hshop':
            self.shop = SHOP(self.problem, no_duplicate_search=True,
                        hierarchical_plan=True, poset_inc_impl=False)
            output = output_ipc2020_hierarchical
        elif self.alg.lower() == 'hshopi':
            self.shop = SHOP(self.problem, no_duplicate_search=True,
                        hierarchical_plan=True, poset_inc_impl=True)
            output = output_ipc2020_hierarchical
        plan = self.shop.find_plan(self.problem.init, self.problem.goal_task)
        toc = time.process_time()
        self.stats.solving_time = (toc - tic)
        LOGGER.info("SHOP solving duration: %.3f", (toc - tic))

        if plan is None:
            LOGGER.error("No plan found!")
            return
        #else:
        out_plan = open(f"plan-{self.alg.lower()}.plan", "w", encoding="utf-8")
        output(plan, out_plan)
        out_plan.close()

def build_problem(domain, problem):
    tic = time.process_time()
    LOGGER.info("Parsing PDDL domain %s", domain)
    pddl_domain = pddl.parse_domain(domain, file_stream=True)
    LOGGER.info("Parsing PDDL problem %s", problem)
    pddl_problem = pddl.parse_problem(problem, file_stream=True)
    toc = time.process_time()
    LOGGER.info("parsing duration: %.3f", (toc - tic))
    stats = Statistics(pddl_domain.name, pddl_problem.name, '')
    stats.parsing_time = (toc - tic)
    tic = time.process_time()
    LOGGER.info("Building HiPOP problem")
    shop_problem = Problem(pddl_problem, pddl_domain,
                           filter_static=True, tdg_filter_useless=True,
                           htn_problem=True)
    toc = time.process_time()
    stats.problem_time = (toc - tic)
    LOGGER.info("building problem duration: %.3f", (toc - tic))
    return shop_problem, stats

def verify(domain, problem, plan):
    verificator = subprocess.Popen(["../pandaPIparser/pandaPIparser",
                                    "-verify",
                                    domain,
                                    problem,
                                    plan], stdout=subprocess.PIPE)
    verification = verificator.stdout.read().decode(encoding='utf-8')
    return verification.count("true")


def process_problem(pddl_domain, pddl_problem, problem, timeout, stats):
    results = [deepcopy(stats), deepcopy(stats), deepcopy(stats)]
    shop_thread = SolveThread(problem, 'shop', results[0])
    hshop_thread = SolveThread(problem, 'hshop', results[1])
    hshopi_thread = SolveThread(problem, 'hshopi', results[2])
    shop_thread.start()
    hshop_thread.start()
    hshopi_thread.start()
    hshop_thread.join(timeout=timeout)
    shop_thread.join()
    hshopi_thread.join()
    if shop_thread.is_alive():
        LOGGER.error("SHOP timed-out on problem %s", problem.name)
        shop_thread.terminate()
    else:
        results[0].verif = verify(pddl_domain, pddl_problem, 'plan-shop.plan')
    if hshop_thread.is_alive():
        LOGGER.error("HSHOP timed-out on problem %s", problem.name)
        hshop_thread.terminate()
    else:
        results[1].verif = verify(pddl_domain, pddl_problem, 'plan-hshop.plan')
    if hshopi_thread.is_alive():
        LOGGER.error("HSHOP-INC timed-out on problem %s", problem.name)
        hshopi_thread.terminate()
    else:
        results[2].verif = verify(pddl_domain, pddl_problem, 'plan-hshopi.plan')
    for r in results: print(r)
    return results

def process_domain(benchmark, bench_root, max_bench, timeout):
    root = os.path.join(bench_root, benchmark)
    domain = next(Path(os.path.join(root, 'domains')).rglob('*.?ddl'))
    bench = 1
    results = []
    problems = []
    for problem in sorted(Path(os.path.join(root, 'problems')).rglob('*.?ddl')):
        problems.append(problem)
        pb, stats = build_problem(domain, problem)
        results.append(process_problem(domain, problem, pb, timeout, stats))
        bench += 1
        if bench > max_bench:
            break
    return problems, results

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SHOP planner")
    parser.add_argument("benchmark", help="Benchmark name", type=str)
    parser.add_argument("-N", "--nb-problems", default=math.inf,
                        help="Number of problems to solve", type=int)
    parser.add_argument("-T", "--timeout", default=None,
                        help="Timeout in seconds", type=int)
    parser.add_argument("-p", "--benchmark-prefix", help="Prefix path to benchmarks",
                        type=str)
    parser.add_argument("-P", "--plot", help="Plot results", action="store_true")
    args = parser.parse_args()

    setup()
    if args.benchmark_prefix:
        bench_root = args.benchmark_prefix
    else:
        bench_root = os.path.join('..', 'benchmarks', 'ipc2020-hierarchical', 'HDDL-total')
    problems, results = process_domain(args.benchmark, bench_root, args.nb_problems, args.timeout)
    if args.plot:
        plt.plot(range(len(problems)), [(x[0].solving_time if x[0].verif else None) for x in results], 'r-x', label="SHOP")
        plt.plot(range(len(problems)), [(x[1].solving_time if x[1].verif else None) for x in results], 'b-o', label="H-SHOP")
        plt.plot(range(len(problems)), [(x[2].solving_time if x[2].verif else None) for x in results], 'g-s', label="H-SHOP-INC")
        plt.xticks([x for x in range(len(problems))], [f"{x+1}" for x in range(len(problems))])
        plt.xlabel("problem")
        plt.ylabel("solving time (s)")
        plt.title(args.benchmark)
        plt.legend()
        plt.show()
