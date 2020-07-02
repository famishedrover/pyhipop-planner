import logging
import math
import io
import subprocess
import time
import os
from pathlib import Path

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
        shop = SHOP(shop_problem, no_duplicate_search=False, hierarchical_plan=False)
        output = output_ipc2020_flat
    elif alg.lower() == 'hshop':
        shop = SHOP(shop_problem, no_duplicate_search=False, hierarchical_plan=True)
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
    #print(verification)
    stats.verif = verification.count("true")
    return stats

def process_domain(benchmark, bench_root, max_bench=math.inf):
    root = os.path.join(bench_root, benchmark)
    domain = next(Path(os.path.join(root, 'domains')).rglob('*.?ddl'))
    bench = 1
    for problem in sorted(Path(os.path.join(root, 'problems')).rglob('*.?ddl')):
        print(process_problem(domain, problem, 'shop'))
        print(process_problem(domain, problem, 'hshop'))
        bench += 1
        if bench > max_bench:
            return

if __name__ == '__main__':
    setup()
    bench_root = os.path.join('..', 'benchmarks', 'ipc2020-hierarchical', 'HDDL-total')
    process_domain('transport', bench_root, 7)
    process_domain('rover', bench_root, 7)
