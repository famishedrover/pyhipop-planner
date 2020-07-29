from io import TextIOBase
from typing import Union, List
from ..search.plan import HierarchicalPartialPlan

def output_ipc2020_hierarchical(plan: HierarchicalPartialPlan,
                                out_stream: TextIOBase):
    out_stream.write("==>\n")
    index_map = {}
    step_index = 1
    # Action sequence
    seq_plan = list(plan.sequential_plan())
    for step in seq_plan:
        if step[0] not in plan.tasks:
            if '__init' in step[1].operator: continue
            if '__goal' in step[1].operator: continue
            index_map[step[0]] = step_index
            out_stream.write(f"{step_index} {step[1].operator}\n")
            step_index += 1
    # Tasks
    for task in plan.tasks:
        index_map[task] = step_index
        step_index += 1
        if '__top' in plan.get_step(task).operator:
            root_task = task
    method, subtasks = plan.get_decomposition(root_task)
    subtasks_set = set(subtasks)
    root_subtasks = [index_map[x] for x, _ in seq_plan if x in subtasks_set]
    out_stream.write(f"root {' '.join(map(str, root_subtasks))}\n")
    # Hierarchy
    for task in plan.tasks:
        if task == root_task:
            continue
        try:
            method, subtasks = plan.get_decomposition(task)
        except KeyError:
            continue
        subtasks_set = set(subtasks)
        root_subtasks = [index_map[x] for x, _ in seq_plan if x in subtasks_set]
        out_stream.write(f"{index_map[task]} {plan.get_step(task).operator} -> {method} ")
        out_stream.write(" ".join(map(str, root_subtasks)))
        out_stream.write("\n")
        step_index += 1
    # End
    out_stream.write("<==\n")

def output_ipc2020_flat(plan: List[str],
                        out_stream: TextIOBase):
    out_stream.write("==>\n")
    # Action sequence
    for step in range(len(plan)):
        out_stream.write(f"{step} {plan[step]}\n")
    # End
    out_stream.write("<==\n")
