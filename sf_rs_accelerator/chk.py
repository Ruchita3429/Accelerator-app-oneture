import ast, sys
with open(r'c:\Users\himan\Downloads\sf_rs_accelerator-20260902T162545Z-1-001\sf_rs_accelerator\app_final.py','r',encoding='utf-8') as f:
    lines = f.readlines()
    src = ''.join(lines)
try:
    ast.parse(src)
    result = 'OK'
except SyntaxError as e:
    ctx = lines[max(0,e.lineno-3):min(len(lines),e.lineno+2)]
    ctx_str = ''.join(f"{e.lineno-2+i}: {l}" for i,l in enumerate(ctx))
    result = f'LINE {e.lineno}: {e.msg}\nContext:\n{ctx_str}'
with open('chk_result.txt','w',encoding='utf-8') as f:
    f.write(result)
