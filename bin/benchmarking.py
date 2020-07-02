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

    def __init__(self, domain, problem, alg, results):
        threading.Thread.__init__(self)
        self.domain = domain
        self.problem = problem
        self.alg = alg
        self.results = results

    def terminate(self):
        self.shop.stop()
        self.join()

    def run(self):
        tic = time.process_time()
        LOGGER.info("Parsing PDDL domain %s", self.domain)
        pddl_domain = pddl.parse_domain(self.domain, file_stream=True)
        LOGGER.info("Parsing PDDL problem %s", self.problem)
        pddl_problem = pddl.parse_problem(self.problem, file_stream=True)
        toc = time.process_time()
        LOGGER.info("parsing duration: %.3f", (toc - tic))

        stats = Statistics(pddl_domain.name, pddl_problem.name, self.alg)
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
        if self.alg.lower() == 'shop':
            self.shop = SHOP(shop_problem, no_duplicate_search=True, hierarchical_plan=False)
            output = output_ipc2020_flat
        elif self.alg.lower() == 'hshop':
            self.shop = SHOP(shop_problem, no_duplicate_search=True,
                        hierarchical_plan=True, poset_inc_impl=False)
            output = output_ipc2020_hierarchical
        elif self.alg.lower() == 'hshop-inc':
            self.shop = SHOP(shop_problem, no_duplicate_search=True,
                        hierarchical_plan=True, poset_inc_impl=True)
            output = output_ipc2020_hierarchical
        plan = self.shop.find_plan(shop_problem.init, shop_problem.goal_task)
        toc = time.process_time()
        stats.solving_time = (toc - tic)
        LOGGER.info("SHOP solving duration: %.3f", (toc - tic))

        if plan is None:
            LOGGER.error("No plan found!")
            return
        #else:
        out_plan = open("plan.plan", "w", encoding="utf-8")
        output(plan, out_plan)
        out_plan.close()

        verificator = subprocess.Popen(["../pandaPIparser/pandaPIparser",
                                        "-verify",
                                        self.domain,
                                        self.problem,
                                        "plan.plan"], stdout=subprocess.PIPE)
        verification = verificator.stdout.read().decode(encoding='utf-8')
        stats.verif = verification.count("true")
        self.results.append(stats)

def process_alg(domain, problem, algorithm, data, timeout):
    alg_thread = SolveThread(domain, problem, algorithm, data)
    alg_thread.start()
    alg_thread.join(timeout=timeout)
    if alg_thread.is_alive():
        LOGGER.error("Alg %s timed-out on problem %s", algorithm, problem)
        alg_thread.terminate()
        data.append(Statistics(domain.name, problem.name, algorithm))
    print(data[-1])

def process_domain(benchmark, bench_root, max_bench, timeout):
    root = os.path.join(bench_root, benchmark)
    domain = next(Path(os.path.join(root, 'domains')).rglob('*.?ddl'))
    bench = 1
    datas = defaultdict(list)
    datas['shop'] = []
    datas['hshop'] = []
    datas['hshop-inc'] = []
    problems = []
    for problem in sorted(Path(os.path.join(root, 'problems')).rglob('*.?ddl')):
        problems.append(problem)
        for alg, data in datas.items():
            process_alg(domain, problem, alg, data, timeout)
        bench += 1
        if bench > max_bench:
            break
    return problems, datas

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
        plt.plot(range(len(problems)), [x.solving_time for x in results['shop']], 'r-x', label="SHOP")
        plt.plot(range(len(problems)), [x.solving_time for x in results['hshop']], 'b-o', label="H-SHOP")
        plt.plot(range(len(problems)), [x.solving_time for x in results['hshop-inc']], 'g-s', label="H-SHOP-INC")
        plt.xticks([x for x in range(len(problems))], [f"{x+1}" for x in range(len(problems))])
        plt.xlabel("problem")
        plt.ylabel("solving time (s)")
        plt.title(args.benchmark)
        plt.legend()
        plt.show()
