import subprocess
import sys
import os
import time
import logging

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("process-runner")

python = sys.executable
SRC_DIR = os.path.dirname(os.path.abspath(__file__))  # src directory

# Set PYTHONPATH to include src directory
env = os.environ.copy()
env['PYTHONPATH'] = SRC_DIR + os.pathsep + env.get('PYTHONPATH', '')

processes = []

def start_process(name, command, cwd=SRC_DIR):
    logger.info(f"▶️  Starting {name}...")
    try:
        p = subprocess.Popen(
            command,
            env=env,
            cwd=cwd
        )
        processes.append((name, p))
        return p
    except Exception as e:
        logger.error(f"Failed to start {name}: {e}")
        return None

try:
    # 1. Email Extractor
    start_process("email-extract-app", [python, os.path.join(SRC_DIR, "email-extract-app.py")])
    time.sleep(1)

    # 2. OpenSearch Extractor
    start_process("opensearch-extract-app", [python, os.path.join(SRC_DIR, "opensearch-extract-app.py")])
    time.sleep(1)

    # 3. Reminder Scheduler
    start_process("remainder_scheduler", [python, os.path.join(SRC_DIR, "remainder_scheduler.py")])
    time.sleep(1)
    
    # 4. FastAPI
    logger.info("▶️  Starting ops_solution.py (FastAPI)...")
    # Using list for command avoids shell=True
    p4 = subprocess.Popen(
        [python, "-m", "uvicorn", "ops_solution:app", "--reload", "--host", "127.0.0.1", "--port", "8000"],
        env=env,
        cwd=SRC_DIR
    )
    processes.append(("ops_solution FastAPI", p4))
    time.sleep(2)

    # 5. Error Solution Creator
    start_process("error-solution-create", [python, os.path.join(SRC_DIR, "error-solution-create.py")])

    print("\n" + "=" * 60)
    logger.info("✅ All services started successfully!")
    print("=" * 60)
    print("\n📋 Running Services:")
    print("  1. Email Extractor        → Extracts errors from email")
    print("  2. OpenSearch Extractor   → Extracts errors from OpenSearch")
    print("  3. Reminder Scheduler     → Sends reminder emails")
    print("  4. FastAPI (port 8000)    → http://127.0.0.1:8000")
    print("  5. Error Processor        → Consumes RabbitMQ")
    print("\n🛑 Press CTRL+C to stop all services\n")
    print("=" * 60)
    
    # Wait for all processes
    for name, p in processes:
        if p:
            p.wait()

except KeyboardInterrupt:
    print("\n\n" + "=" * 60)
    logger.info("⛔ Stopping all processes...")
    print("=" * 60)
    
    for name, p in processes:
        if p:
            logger.info(f"  Stopping {name}...")
            p.terminate()
    
    # Wait for clean shutdown
    for name, p in processes:
        if p:
            try:
                p.wait(timeout=5)
                logger.info(f"  ✓ {name} stopped")
            except subprocess.TimeoutExpired:
                logger.warning(f"  ⚠️  Force killing {name}")
                p.kill()
    
    print("=" * 60)
    logger.info("✅ All services stopped")
    print("=" * 60)

except Exception as e:
    logger.error(f"❌ Error: {e}", exc_info=True)
    
    # Clean up on error
    for name, p in processes:
        if p:
            p.terminate()
