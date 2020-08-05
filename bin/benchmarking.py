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

def solve(domain, problem, options, timeout, stats):
    LOGGER.info("Solving problem %s with %s", problem, options)
    tic = time.time()
    result = subprocess.run(['python3', '-m', 'hipop', domain, problem] + options,
                    timeout=timeout,
                    stdout=subprocess.PIPE, encoding='utf-8')
    toc = time.time()
    LOGGER.info("- duration: %.3f", (toc - tic))
    stats.solving_time = (toc-tic)
    f = open('plan.plan', 'w')
    f.write(result.stdout)
    f.close()
    LOGGER.info("- result: %s", result)
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

def process_problem(pddl_domain, pddl_problem,
                    options, timeout, stats, panda_prefix):
    results = []
    results.append(deepcopy(stats))
    try:
        if solve(pddl_domain, pddl_problem, options, timeout, results[0]):
            results[0].verif = verify(pddl_domain, pddl_problem, 'plan.plan', panda_prefix)
    except subprocess.TimeoutExpired:
        pass
    for r in results: print(r)
    return results

def process_domain(benchmark, bench_root,
                   max_bench, options, timeout, panda_prefix):
    root = os.path.join(bench_root, benchmark)
    domain = next(Path(os.path.join(root, 'domains')).rglob('*.?ddl'))
    bench = 1
    results = []
    problems = []
    for problem in sorted(Path(os.path.join(root, 'problems')).rglob('*.?ddl')):
        problems.append(problem)
        pb, stats = build_problem(domain, problem)
        print(f" -- problem {pb.name} of {pb.domain}")
        results.append(process_problem(domain, problem,
                                       options, timeout, 
                                       stats, panda_prefix))
        bench += 1
        if bench > max_bench:
            break
    return problems, results

if __name__ == '__main__':
    hoptions = """heuristic options:
        0: single queue with f
        1: shoplike
        2: double queue: f, htdg
        3: double queue: f, hadd
        4: double queue: hadd, htdg
        5: double queue: hadd, htdg_max
        6: double queue: hadd, htdg_min
        7: double queue: hadd, htdg_max + hadd
        8: double queue: htdg, htdg_max + hadd
        9: double queue: f, htdg_max + hadd
        10: double queue: f, htdg_min + hadd
        11: double queue: htdg, htdg_min + hadd
    """

    parser = argparse.ArgumentParser(description="SHOP planner", formatter_class=argparse.RawTextHelpFormatter, epilog=hoptions)
    parser.add_argument("benchmark", help="Benchmark name", type=str,
                        choices=BENCHMARKS.keys())
    parser.add_argument("-N", "--nb-problems", default=math.inf,
                        help="Number of problems to solve", type=int)
    parser.add_argument("-o", "--option", type=int, help="heuristic choice. see choices below")
    parser.add_argument("-p", "--ipc2020-prefix", dest='prefix',
                        help="Prefix path to IPC2020 benchmarks",
                        default=os.path.join('..', 'ipc2020-domains'))
    parser.add_argument("-P", "--plot", help="Plot results", action="store_true")
    parser.add_argument("--panda-prefix",
                        help="Prefix path to PANDA verifier",
                        default=os.path.join('..', 'pandaPIparser'))
    parser.add_argument("-T", "--timeout", default=None,
                        help="Timeout in seconds", type=int)

    switcher = {
        0: [""], # single queue with f
        1: ["--shoplike"],
        2: ["--dq"], # default is h1 = 'htdg', h2 = 'f'
        3: ["--dq", "-h1", "hadd", "-h2", "f"],
        4: ["--dq", "-h1", "hadd", "-h2", "htdg"],
        5: ["--dq", "-h1", "hadd", "-h2", "htdg_max"],
        6: ["--dq", "-h1", "hadd", "-h2", "htdg_min"],
        7: ["--dq", "-h1", "hadd", "-h2", "htdg_max_deep"],
        8: ["--dq", "-h1", "htdg", "-h2", "htdg_max_deep"],
        9: ["--dq", "-h1", "htdg_max_deep", "-h2", "f"],
        10: ["--dq", "-h1", "htdg_mmin_deep", "-h2", "f"],
        11: ["--dq", "-h1", "htdg_min_deep", "-h2", "htdg"],
    }
    args = parser.parse_args()
    options = switcher.get(args.option)
    setup()
    if args.prefix:
        bench_root = args.prefix
    else:
        bench_root = os.path.join('..', 'ipc2020-domains')
    problems, results = process_domain(BENCHMARKS[args.benchmark],
                                       bench_root,
                                       args.nb_problems, 
                                       options, 
                                       args.timeout,
                                       args.panda_prefix)
    if args.plot:
        color_codes = map('C{}'.format, cycle(range(10)))
        marker = cycle(('+', '.', 'o', '*'))
        plt.plot(range(len(problems)), [(x[0].solving_time if x[0].verif else None) for x in results],
                color=next(color_codes), marker=next(marker), label='HiPOP')
        plt.xticks([x for x in range(len(problems))], [f"{x+1}" for x in range(len(problems))])
        plt.xlabel("problem")
        plt.ylabel("solving time (s)")
        plt.title(args.benchmark)
        plt.legend()
        plt.show()


