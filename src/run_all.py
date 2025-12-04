import subprocess
import sys
import os
import time

python = sys.executable
SRC_DIR = os.path.dirname(os.path.abspath(__file__))  # src directory

# Set PYTHONPATH to include src directory
env = os.environ.copy()
env['PYTHONPATH'] = SRC_DIR + os.pathsep + env.get('PYTHONPATH', '')

print("=" * 60)
print("🚀 Starting All Services")
print("=" * 60)
print(f"Python: {python}")
print(f"Source Dir: {SRC_DIR}")
print(f"PYTHONPATH: {env['PYTHONPATH']}")
print("=" * 60)

processes = []

try:
    # 1. ELK Poller
    print("▶️  Starting error-extract-app.py...")
    p1 = subprocess.Popen(
        [python, os.path.join(SRC_DIR, "error-extract-app.py")],
        env=env,
        cwd=SRC_DIR
    )
    processes.append(("error-extract-app", p1))
    time.sleep(1)
    
    # 2. Reminder Scheduler
    print("▶️  Starting remainder_scheduler.py...")
    p2 = subprocess.Popen(
        [python, os.path.join(SRC_DIR, "remainder_scheduler.py")],
        env=env,
        cwd=SRC_DIR
    )
    processes.append(("remainder_scheduler", p2))
    time.sleep(1)
    
    # 3. FastAPI - FIXED: Run from src directory without module prefix
    print("▶️  Starting ops_solution.py (FastAPI)...")
    p3 = subprocess.Popen(
        [python, "-m", "uvicorn", "ops_solution:app", "--reload", "--host", "0.0.0.0", "--port", "8000"],
        env=env,
        cwd=SRC_DIR  # ✅ Run from src directory
    )
    processes.append(("ops_solution FastAPI", p3))
    time.sleep(2)
    
    # 4. Error Solution Creator
    print("▶️  Starting error-solution-create.py...")
    p4 = subprocess.Popen(
        [python, os.path.join(SRC_DIR, "error-solution-create.py")],
        env=env,
        cwd=SRC_DIR
    )
    processes.append(("error-solution-create", p4))
    
    print("\n" + "=" * 60)
    print("✅ All services started successfully!")
    print("=" * 60)
    print("\n📋 Running Services:")
    print("  1. ELK Poller         → Extracts errors from ELK")
    print("  2. Reminder Scheduler → Sends reminder emails")
    print("  3. FastAPI (port 8000)→ http://127.0.0.1:8000")
    print("  4. Error Processor    → Consumes RabbitMQ")
    print("\n🛑 Press CTRL+C to stop all services\n")
    print("=" * 60)
    
    # Wait for all processes
    for name, p in processes:
        p.wait()

except KeyboardInterrupt:
    print("\n\n" + "=" * 60)
    print("⛔ Stopping all processes...")
    print("=" * 60)
    
    for name, p in processes:
        print(f"  Stopping {name}...")
        p.terminate()
    
    # Wait for clean shutdown
    for name, p in processes:
        try:
            p.wait(timeout=5)
            print(f"  ✓ {name} stopped")
        except subprocess.TimeoutExpired:
            print(f"  ⚠️  Force killing {name}")
            p.kill()
    
    print("=" * 60)
    print("✅ All services stopped")
    print("=" * 60)

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    
    # Clean up on error
    for name, p in processes:
        p.terminate()