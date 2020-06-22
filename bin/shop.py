import sys
import os
import argparse
import logging
import time
import itertools

import pddl
from hipop.problem.problem import Problem
from hipop.search.search import SHOP
from hipop.utils.profiling import start_profiling, stop_profiling

LOGGER = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="SHOP planner")
    parser.add_argument("domain", help="PDDL domain file", type=str)
    parser.add_argument("problem", help="PDDL problem file", type=str)
    parser.add_argument("-d", "--debug", help="Activate debug logs",
                        action='store_const', dest="loglevel",
                        const=logging.DEBUG, default=logging.WARNING)
    parser.add_argument("-v", "--verbose", help="Activate verbose logs",
                        action='store_const', dest="loglevel",
                        const=logging.INFO, default=logging.WARNING)
    parser.add_argument("--trace-malloc", help="Activate tracemalloc",
                        action='store_true')
    parser.add_argument("--profile", help="Activate profiling",
                        action='store_true')
    args = parser.parse_args()

    logformat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(stream=sys.stderr,
                        level=args.loglevel,
                        format=logformat)

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
    problem = Problem(pddl_problem, pddl_domain)
    toc = time.process_time()
    LOGGER.warning("building problem duration: %.3f", (toc - tic))
    LOGGER.info("nb actions: %d", len(problem.actions))
    LOGGER.info("nb tasks: %d", len(problem.tasks))
    LOGGER.info("nb methods: %d", sum(1 for task in problem.tasks for _ in task.methods))
    LOGGER.info("init state size: %d", len(problem.init))

    stop_profiling(args.trace_malloc, profiler)

    LOGGER.info("Root subtasks in order:")
    goal = problem.goal_task
    for subtask_name in goal.sorted_tasks:
        LOGGER.info(" - %s", subtask_name)
    subtask_name = goal.subtask(goal.task_network.bottom())
    LOGGER.info("First subtask %s has methods:", subtask_name)
    subtask = problem.get_task(subtask_name)
    for method in subtask.methods:
        LOGGER.info(" - %s instanciated as %s", method.name, method)
    method = list(subtask.methods)[0]
    LOGGER.info("Method %s has subtasks:", method)
    for subtask_name in method.sorted_tasks:
            LOGGER.info(" - %s", subtask_name)
    subtask_name = list(method.sorted_tasks)[2]
    try:
        subtask = problem.get_task(subtask_name)
        LOGGER.info("Subtask %s has methods:", subtask_name)
    except KeyError:
        LOGGER.info("Subtask %s is a primitive action", subtask_name)
        subtask = problem.get_action(subtask_name)
        LOGGER.info(" - pre: %s", subtask.preconditions)
        LOGGER.info(" - eff: %s", subtask.effects)

    LOGGER.info("Solving problem with SHOP")
    tic = time.process_time()
    shop = SHOP(problem)
    plan = shop.find_plan(problem.init,
                          list(goal.sorted_tasks))
    toc = time.process_time()
    LOGGER.warning("SHOP solving duration: %.3f", (toc - tic))
    LOGGER.info("plan: %s", plan)

if __name__ == '__main__':
    main()
