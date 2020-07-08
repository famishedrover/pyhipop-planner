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
import enum
from itertools import cycle

import pddl
from hipop.problem.problem import Problem
from hipop.search.shop import SHOP
from hipop.utils.logger import setup_logging
from hipop.utils.io import output_ipc2020_flat, output_ipc2020_hierarchical

LOGGER = logging.getLogger('benchmarking')

class Algorithms(enum.Enum):
    SHOP = 'shop'
    HSHOP = 'h-shop'
    HSHOPI = 'h-shop-inc'

BENCHMARKS = {
    'transport': os.path.join('total-order-generated', 'Transport'),
    'rover': os.path.join('total-order-generated', 'Rover-PANDA'),
    'satellite': os.path.join('total-order-generated', 'Satellite-PANDA'),
    'smartphone': os.path.join('total-order-generated', 'SmartPhone'),
    'umtranslog': os.path.join('total-order-generated', 'UM-Translog'),
    'woodworking': os.path.join('total-order-generated', 'Woodworking'),
    'zenotravel': os.path.join('total-order-generated', 'Zenotravel'),
    'miconic': os.path.join('total-order', 'Miconic'),
}

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
    setup_logging(level=logging.WARNING)

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
        if self.alg.lower() == Algorithms.SHOP.value:
            self.shop = SHOP(self.problem, no_duplicate_search=True, hierarchical_plan=False)
            output = output_ipc2020_flat
        elif self.alg.lower() == Algorithms.HSHOP.value:
            self.shop = SHOP(self.problem, no_duplicate_search=True,
                        hierarchical_plan=True, poset_inc_impl=False)
            output = output_ipc2020_hierarchical
        elif self.alg.lower() == Algorithms.HSHOPI.value:
            self.shop = SHOP(self.problem, no_duplicate_search=True,
                        hierarchical_plan=True, poset_inc_impl=True)
            output = output_ipc2020_hierarchical
        plan = self.shop.find_plan(self.problem.init, self.problem.goal_task)
        toc = time.process_time()
        self.stats.solving_time = (toc - tic)
        LOGGER.info("SHOP %s solving duration: %.3f", self.alg, (toc - tic))

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
    LOGGER.info("Building problem")
    shop_problem = Problem(pddl_problem, pddl_domain,
                           filter_static=True, tdg_filter_useless=True,
                           htn_problem=True)
    toc = time.process_time()
    stats.problem_time = (toc - tic)
    LOGGER.info("building problem duration: %.3f", (toc - tic))
    return shop_problem, stats

def verify(domain, problem, plan, prefix):
    verificator = subprocess.Popen([os.path.join(prefix, "pandaPIparser"),
                                    "-verify",
                                    domain,
                                    problem,
                                    plan], stdout=subprocess.PIPE)
    verification = verificator.stdout.read().decode(encoding='utf-8')
    return verification.count("true")


def process_problem(pddl_domain, pddl_problem, problem, algorithms,
                    timeout, stats, panda_prefix):
    results = []
    for i in range(len(algorithms)):
        results.append(deepcopy(stats))
    threads = []
    for i in range(len(algorithms)):
        threads.append(SolveThread(problem, algorithms[i], results[i]))
    for th in threads:
        th.start()
    tic = time.process_time()
    for th in threads:
        toc = time.process_time()
        th.join(timeout=(timeout - (toc - tic)) if timeout else None)
    for i in range(len(algorithms)):
        if threads[i].is_alive():
            LOGGER.error("Thread %s timed-out on problem %s",
                         algorithms[i], problem.name)
            th.terminate()
        else:
            results[i].verif = verify(pddl_domain, pddl_problem,
                                      f'plan-{algorithms[i]}.plan',
                                      panda_prefix)
    for r in results: print(r)
    return results

def process_domain(benchmark, algorithms, bench_root,
                   max_bench, timeout, panda_prefix):
    root = os.path.join(bench_root, benchmark)
    domain = next(Path(os.path.join(root, 'domains')).rglob('*.?ddl'))
    bench = 1
    results = []
    problems = []
    for problem in sorted(Path(os.path.join(root, 'problems')).rglob('*.?ddl')):
        problems.append(problem)
        pb, stats = build_problem(domain, problem)
        results.append(process_problem(domain, problem, pb, algorithms,
                                       timeout, stats, panda_prefix))
        bench += 1
        if bench > max_bench:
            break
    return problems, results

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SHOP planner")
    parser.add_argument("benchmark", help="Benchmark name", type=str,
                        choices=BENCHMARKS.keys())
    parser.add_argument("-N", "--nb-problems", default=math.inf,
                        help="Number of problems to solve", type=int)
    parser.add_argument("-T", "--timeout", default=None,
                        help="Timeout in seconds", type=int)
    parser.add_argument("-p", "--ipc2020-prefix", dest='prefix',
                        help="Prefix path to IPC2020 benchmarks",
                        default=os.path.join('..', 'ipc2020-domains'))
    parser.add_argument("-P", "--plot", help="Plot results", action="store_true")
    parser.add_argument("--panda-prefix",
                        help="Prefix path to PANDA verifier",
                        default=os.path.join('..', 'pandaPIparser'))
    parser.add_argument("-a", "--algorithms", nargs='+',
                        default=[a.value for a in Algorithms],
                        choices=[a.value for a in Algorithms])
    args = parser.parse_args()

    setup()
    if args.prefix:
        bench_root = args.prefix
    else:
        bench_root = os.path.join('..', 'ipc2020-domains')
    problems, results = process_domain(BENCHMARKS[args.benchmark],
                                       args.algorithms, bench_root,
                                       args.nb_problems, args.timeout,
                                       args.panda_prefix)
    if args.plot:
        color_codes = map('C{}'.format, cycle(range(10)))
        marker = cycle(('+', '.', 'o', '*'))
        for i in range(len(args.algorithms)):
            plt.plot(range(len(problems)), [(x[i].solving_time if x[i].verif else None) for x in results],
                     color=next(color_codes), marker=next(marker), label=args.algorithms[i].upper())
        plt.xticks([x for x in range(len(problems))], [f"{x+1}" for x in range(len(problems))])
        plt.xlabel("problem")
        plt.ylabel("solving time (s)")
        plt.title(args.benchmark)
        plt.legend()
        plt.show()
