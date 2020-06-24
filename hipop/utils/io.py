from io import TextIOBase

def output_ipc2020(plan, out_stream: TextIOBase):
    out_stream.write("==>\n")
    # Action sequence
    for i in range(len(plan)):
        out_stream.write(f"{i} {plan[i]}\n")
    out_stream.write("<==\n")
