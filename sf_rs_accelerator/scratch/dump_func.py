import sys, marshal, dis
sys.stdout.reconfigure(encoding='utf-8')

with open('__pycache__/tab_data_validation.cpython-314.pyc', 'rb') as f:
    f.seek(16)
    code = marshal.load(f)

# Extract _RS_TO_SF_TYPE by running module up to STORE_NAME '_RS_TO_SF_TYPE'
globs = {}
# Find line number where _RS_TO_SF_TYPE is stored
for inst in dis.get_instructions(code):
    if inst.opname == 'STORE_NAME' and inst.argval == '_RS_TO_SF_TYPE':
        break

# Let's inspect the code objects
funcs = {}
for const in code.co_consts:
    if hasattr(const, 'co_name') and not const.co_name.startswith('__'):
        funcs[const.co_name] = const

def dump_func(name):
    c = funcs[name]
    print(f"\n{'='*70}\nFUNCTION: {name} (lines {c.co_firstlineno})\n{'='*70}")
    print("ARGS:", c.co_varnames[:c.co_argcount])
    print("CONSTS:", [x if not hasattr(x, 'co_name') else f"<code: {x.co_name}>" for x in c.co_consts])
    print("NAMES:", c.co_names)
    print("VARNAMES:", c.co_varnames)
    print("FREEVARS:", c.co_freevars)
    print("CELLVARS:", c.co_cellvars)
    curr_line = None
    for inst in dis.get_instructions(c):
        if inst.starts_line and inst.starts_line != curr_line:
            curr_line = inst.starts_line
            print(f"\n--- Line {curr_line} ---")
        arg_val = repr(inst.argval) if inst.argval is not None else ""
        print(f"  {inst.offset:4d}  {inst.opname:<24} {inst.argrepr:<30} {arg_val}")

for fn in sys.argv[1:]:
    if fn in funcs:
        dump_func(fn)
