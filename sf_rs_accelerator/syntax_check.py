import py_compile, sys
try:
    py_compile.compile(r'c:\Users\himan\Downloads\sf_rs_accelerator-20260902T162545Z-1-001\sf_rs_accelerator\chunk_02_job_queue_engine.py', doraise=True)
    with open('syntax_result.txt', 'w') as f:
        f.write('SYNTAX OK')
except py_compile.PyCompileError as e:
    with open('syntax_result.txt', 'w') as f:
        f.write(f'SYNTAX ERROR:\n{e}')