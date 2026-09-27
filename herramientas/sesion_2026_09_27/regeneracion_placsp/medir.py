import resource, subprocess, sys, time
t = time.time()
r = subprocess.run(sys.argv[1:])
print(f"MEDIDA: {time.time() - t:.0f} s, pico de memoria {resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024:,.0f} MB, exit {r.returncode}")
sys.exit(r.returncode)
