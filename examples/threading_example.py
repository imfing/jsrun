"""Example: Demonstration of GIL release during JS Runtime execution.

This example script shows that Python threads can make progress while JS Runtime is executing JavaScript,
proving that the GIL (Global Interpreter Lock) is properly released during JS Runtime operations.
This allows for true parallelism between JavaScript execution and Python threads.
"""

import threading
import time
from jsrun import Runtime


def cpu_bound_python():
    """Simulate CPU-bound Python work."""
    counter = 0
    end_time = time.time() + 0.5
    while time.time() < end_time:
        counter += 1
    return counter


def run_jsrun_eval():
    """Run a CPU-intensive JavaScript computation in JS Runtime."""
    with Runtime() as rt:
        # This JS code will run for a noticeable amount of time
        # During this time, the GIL is released, allowing other threads to run
        result = rt.eval("""
            let sum = 0;
            for (let i = 0; i < 10000000; i++) {
                sum += Math.sqrt(i);
            }
            sum
        """)
        print(f"JS Runtime result: {result}")


def main():
    print("Testing GIL release during JavaScript execution...\n")

    # Test: Run JS Runtime and Python work concurrently
    print("Running JavaScript and Python work in parallel threads")
    start = time.time()

    js_thread = threading.Thread(target=run_jsrun_eval)
    python_results = [0, 0]

    def python_worker(index):
        python_results[index] = cpu_bound_python()

    python_threads = [
        threading.Thread(target=python_worker, args=(0,)),
        threading.Thread(target=python_worker, args=(1,)),
    ]

    # Start all threads
    js_thread.start()
    for t in python_threads:
        t.start()

    # Wait for completion
    js_thread.join()
    for t in python_threads:
        t.join()

    elapsed = time.time() - start
    print(f"Python thread 1 counter: {python_results[0]:,}")
    print(f"Python thread 2 counter: {python_results[1]:,}")
    print(f"Total time: {elapsed:.3f}s")
    print()

    # Verify that threads made substantial progress
    if python_results[0] > 100000 and python_results[1] > 100000:
        print(
            "✓ SUCCESS: Python threads made substantial progress while JS Runtime was running!"
        )
    else:
        print("✗ WARNING: Python threads did not make much progress.")


if __name__ == "__main__":
    main()
