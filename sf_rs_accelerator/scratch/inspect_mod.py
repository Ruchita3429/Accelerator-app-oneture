import sys, marshal, dis
sys.stdout.reconfigure(encoding='utf-8')

with open('__pycache__/tab_data_validation.cpython-314.pyc', 'rb') as f:
    f.seek(16)
    code = marshal.load(f)

print('=== MODULE DOCSTRING ===')
print(code.co_consts[0])

insts = list(dis.get_instructions(code))
for i, inst in enumerate(insts):
    if inst.argval == '_RS_TO_SF_TYPE':
        print(f"Found _RS_TO_SF_TYPE at instruction {i}")
        for j in range(max(0, i-60), min(len(insts), i+5)):
            line_str = f"L{insts[j].starts_line}:" if insts[j].starts_line else "    "
            print(f"{line_str} {insts[j].opname} {repr(insts[j].argval)}")
        break
