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
from hipop.utils.logger import setup_logging

LOGGER = logging.getLogger('benchmarking')

class Algorithms(enum.Enum):
    SHOP = 'shop'
    HSHOP = 'h-shop'
    HSHOPI = 'h-shop-inc'
    HIPOP = 'hipop'

BENCHMARKS = {
    'transport': os.path.join('total-order-generated', 'Transport'),
    'rover': os.path.join('total-order-generated', 'Rover-PANDA'),
    'satellite': os.path.join('total-order-generated', 'Satellite-PANDA'),
    'smartphone': os.path.join('total-order-generated', 'SmartPhone'),
    'umtranslog': os.path.join('total-order-generated', 'UM-Translog'),
    'woodworking': os.path.join('total-order-generated', 'Woodworking'),
    'zenotravel': os.path.join('total-order-generated', 'Zenotravel'),
    'miconic': os.path.join('total-order', 'Miconic'),
    'p-rover': os.path.join('partial-order', 'Rover'),
    'p-satellite': os.path.join('partial-order', 'Satellite'),
    'p-smartphone': os.path.join('partial-order', 'SmartPhone'),
    'p-transport': os.path.join('partial-order', 'Transport'),
    'p-umtranslog': os.path.join('partial-order', 'UM-Translog'),
    'p-woodworking': os.path.join('partial-order', 'Woodworking'),
    'p-zenotravel': os.path.join('partial-order', 'Zenotravel'),
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

def solve(domain, problem, algorithm, timeout, stats):
    LOGGER.info("Solving problem %s with %s", problem, algorithm)
    '''
    if self.alg.lower() == Algorithms.SHOP.value:

            self.shop = SHOP(self.problem, no_duplicate_search=True, hierarchical_plan=False)
            output = output_ipc2020_flat
            plan = self.shop.find_plan(self.problem.init, self.problem.goal_task)
        elif self.alg.lower() == Algorithms.HSHOP.value:
            self.shop = SHOP(self.problem, no_duplicate_search=True,
                        hierarchical_plan=True, poset_inc_impl=False)
            output = output_ipc2020_hierarchical
            plan = self.shop.find_plan(self.problem.init, self.problem.goal_task)
        elif self.alg.lower() == Algorithms.HSHOPI.value:
            self.shop = SHOP(self.problem, no_duplicate_search=True,
                        hierarchical_plan=True, poset_inc_impl=True)
            output = output_ipc2020_hierarchical
            plan = self.shop.find_plan(self.problem.init, self.problem.goal_task)
        elif self.alg.lower() == Algorithms.HIPOP.value:
            self.shop = POP(self.problem)
            output = output_ipc2020_hierarchical
            plan = self.shop.solve(self.problem)
    '''
    tic = time.process_time()
    result = subprocess.run(['python3', '-m', 'hipop', domain, problem],
                    timeout=timeout,
                    stdout=subprocess.PIPE, encoding='utf-8')
    toc = time.process_time()
    LOGGER.info("%s duration: %.3f", algorithm, (toc - tic))
    stats.solving_time = (toc-tic)
    f = open('plan.plan', 'w')
    f.write(result.stdout)
    f.close()
    LOGGER.info("result: %s", result)
    return result.returncode == 0

def verify(domain, problem, plan, prefix):
    verificator = subprocess.Popen([os.path.join(prefix, "pandaPIparser"),
                                    "-verify",
                                    domain,
                                    problem,
                                    plan], stdout=subprocess.PIPE)
    verification = verificator.stdout.read().decode(encoding='utf-8')
    return verification.count("true")

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
    shop_problem = Problem(pddl_problem, pddl_domain)
    toc = time.process_time()
    stats.problem_time = (toc - tic)
    LOGGER.info("building problem duration: %.3f", (toc - tic))
    return shop_problem, stats

def process_problem(pddl_domain, pddl_problem, algorithms,
                    timeout, stats, panda_prefix):
    results = []
    for i in range(len(algorithms)):
        results.append(deepcopy(stats))
    for i in range(len(algorithms)):
        try:
            if solve(pddl_domain, pddl_problem, algorithms[i], timeout, results[i]):
                results[i].verif = verify(pddl_domain, pddl_problem, 'plan.plan', panda_prefix)
        except subprocess.TimeoutExpired:
            pass
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
        print(f" -- problem {pb.name} of {pb.domain}")
        results.append(process_problem(domain, problem, algorithms,
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
    #parser.add_argument("-a", "--algorithms", nargs='+',
    #                    default=[a.value for a in Algorithms],
    #                    choices=[a.value for a in Algorithms])
    args = parser.parse_args()

    setup()
    if args.prefix:
        bench_root = args.prefix
    else:
        bench_root = os.path.join('..', 'ipc2020-domains')
    problems, results = process_domain(BENCHMARKS[args.benchmark],
                                       #args.algorithms,
                                       ['hipop'],
                                       bench_root,
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
