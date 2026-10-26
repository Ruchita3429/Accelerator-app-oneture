import sys, marshal, dis
sys.stdout.reconfigure(encoding='utf-8')

with open('__pycache__/tab_data_validation.cpython-314.pyc', 'rb') as f:
    f.seek(16)
    code = marshal.load(f)

funcs = {}
for const in code.co_consts:
    if hasattr(const, 'co_name') and not const.co_name.startswith('__'):
        funcs[const.co_name] = const

def print_decomp_friendly(fn_name):
    c = funcs[fn_name]
    print(f"\n# {'='*60}\n# {fn_name}\n# {'='*60}")
    insts = list(dis.get_instructions(c))
    curr_line = None
    for inst in insts:
        if inst.starts_line and inst.starts_line != curr_line:
            curr_line = inst.starts_line
            print(f"\n# --- Line {curr_line} ---")
        arg = repr(inst.argval) if inst.argval is not None else ""
        if inst.opname.startswith(('LOAD_', 'STORE_', 'CALL', 'POP_JUMP', 'COMPARE', 'BINARY', 'BUILD', 'RETURN')):
            print(f"    {inst.opname:<20} {inst.argrepr:<25} # {arg}")

for name in sys.argv[1:]:
    if name in funcs:
        print_decomp_friendly(name)
