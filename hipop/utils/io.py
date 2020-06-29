from io import TextIOBase

from ..search.plan import HierarchicalPartialPlan

def output_ipc2020(plan: HierarchicalPartialPlan, out_stream: TextIOBase):
    out_stream.write("==>\n")
    # Action sequence
    seq_plan = plan.sequential_plan()
    for step in seq_plan:
        out_stream.write(f"{step[0]} {step[1]}\n")
    # Hierarchy
    for task in plan.tasks:
        method, subtasks = plan.get_decomposition(task)
        out_stream.write(f"{task} {plan.get_step(task)} -> {method} ")
        out_stream.write(" ".join(map(str,subtasks)))
        out_stream.write("\n")
    # End
    out_stream.write("<==\n")
