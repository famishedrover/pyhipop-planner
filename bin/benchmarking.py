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
from hipop.grounding.problem import Problem
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
    setup_logging(level=logging.WARNING, without=['hipop.'])

def solve(domain, problem, options, count, timeout, stats):
    LOGGER.info("Solving problem %s with %s", problem, options)
    tic = time.time()
    result = subprocess.run(options + [domain, problem],
                            timeout=timeout,
                            stdout=subprocess.PIPE, 
                            encoding='utf-8')
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
                    options, c, timeout, stats, panda_prefix):
    results = deepcopy(stats)
    try:
        if solve(pddl_domain, pddl_problem, options, c, timeout, results):
            results.verif = verify(pddl_domain, pddl_problem, 'plan.plan', panda_prefix)
    except subprocess.TimeoutExpired:
        pass
    print(results)
    return results

def process_domain(benchmark, bench_root,
                   max_bench, options, c, timeout, panda_prefix):
    root = os.path.join(bench_root, benchmark)
    domain = next(Path(os.path.join(root, 'domains')).rglob('*.?ddl'))
    bench = 1
    results = defaultdict(list)
    problems = []
    for problem in sorted(Path(os.path.join(root, 'problems')).rglob('*.?ddl')):
        problems.append(problem)
        pb, stats = build_problem(domain, problem)
        print(f" -- problem {pb.name}")
        algs = [# SHOP
                ['shop', 'hipop-shop.py'],
                # DSF/BFS
                #['dfs', 'hipop-search.py', '-a', 'dfs'],
                #['bfs', 'hipop-search.py', '-a', 'bfs']
                ]
        for ol in ['earliest', 'sorted-earliest', 'local-earliest']:  # 'lifo', 'sorted', 'local',
            for plan in ['depth', 'hadd-max']:  # 'bechon'
                for hadd in ['hadd', 'hadd-reuse']:
                    algs.append([f'hipop-{ol}-{plan}-{hadd}',
                                 'hipop-pop.py', 
                                 '--ol', ol, 
                                 '--plan', plan,
                                 '--hadd', hadd])
        for o in algs:
            print(f" -- alg {o[1:]}")
            results[o[0]].append(process_problem(domain, problem,
                                       o[1:], c, timeout,
                                       stats, panda_prefix))
        bench += 1
        if bench > max_bench:
            break
    return problems, results

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="HiPOP benchmarking", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("benchmark", help="Benchmark name", type=str,
                        choices=BENCHMARKS.keys())
    parser.add_argument("-N", "--nb-problems", default=math.inf,
                        help="Number of problems to solve", type=int)
    parser.add_argument("-p", "--ipc2020-prefix", dest='prefix',
                        help="Prefix path to IPC2020 benchmarks",
                        default=os.path.join('..', 'ipc2020-domains'))
    parser.add_argument("-P", "--plot", help="Plot results", action="store_true")
    parser.add_argument("--savefig", help="Save plot results in figure",
                        type=str)
    parser.add_argument("--panda-prefix",
                        help="Prefix path to PANDA verifier",
                        default=os.path.join('..', 'pandaPIparser'))
    parser.add_argument("-T", "--timeout", default=None,
                        help="Timeout in seconds", type=int)

    args = parser.parse_args()
    setup()
    if args.prefix:
        bench_root = args.prefix
    else:
        bench_root = os.path.join('..', 'ipc2020-domains')
    problems, results = process_domain(BENCHMARKS[args.benchmark],
                                       bench_root,
                                       args.nb_problems, 
                                       [],
                                       0,
                                       args.timeout,
                                       args.panda_prefix)
    if args.plot or args.savefig:
        color_codes = map('C{}'.format, cycle(range(10)))
        marker = cycle(('+', '.', 'o', '*', 's', 'x'))
        for alg, res in results.items():
            plt.plot(range(len(problems)), [(x.solving_time if x.verif in [3, 8] else None) for x in res],
                    color=next(color_codes), marker=next(marker), label=alg, fillstyle='none')
        plt.xticks([x for x in range(len(problems))], [f"{x+1}" for x in range(len(problems))])
        plt.xlabel("problem")
        plt.ylabel("solving time (s)")
        plt.title(args.benchmark)
        plt.legend(loc='center left', bbox_to_anchor=(1.04, 0.5))
        if args.savefig:
            plt.savefig(f"{args.savefig}", bbox_inches="tight")
        if args.plot:
            plt.show()



