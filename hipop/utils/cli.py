import argparse

def add_bool_arg(parser: argparse.ArgumentParser, name: str, dest: str, help: str, default: bool = False):
        group = parser.add_mutually_exclusive_group(required=False)
        group.add_argument('--' + name, dest=dest,
                           help=(f"{help} (default)" if default else help),
                           action='store_true')
        group.add_argument('--no-' + name, dest=dest,
                           help=(f"do not {help}" if default 
                                 else f"do not {help} (default)"),
                           action='store_false')
        parser.set_defaults(**{dest: default})
