# Databricks notebook source
from random import random
from time import time
import sys


def throw_dart():
    x = random()
    y = random()

    if (x * x) + (y * y) <= 1:
        return 1

    return 0


def main(iterations: int):
    hits = 0
    start = time()

    for i in range(0, iterations):
        hits = hits + throw_dart()

    end = time()
    pi = (4 * hits) / iterations
    print(pi)
    print(f"Execution time: {end - start} seconds.")


if __name__ == "__main__":
  print(int(sys.argv[1]))
  main(int(sys.argv[1]))

# COMMAND ----------

from random import random
from time import time
from multiprocessing import Pool
import sys


def throw_dart(iterations: int) -> int:
    hits = 0

    for i in range(iterations):
        x = random()
        y = random()

        if (x * x) + (y * y) <= 1:
            hits = hits + 1

    return hits


def main(iterations: int, process_count: int):
    pool = Pool(processes=process_count)
    trials_per_process = [int(iterations / process_count)] * process_count

    start = time()

    hits = pool.map(throw_dart, trials_per_process)
    pi = (sum(hits) * 4) / iterations

    end = time()

    print(pi)
    print(f"Execution time: {end - start} seconds.")


if __name__ == "__main__":
  print(int(sys.argv[1]))
  main(int(sys.argv[1]), 4)

# COMMAND ----------

# Databricks notebook source
from random import random


def throw_dart(iteraction):
    x = random()
    y = random()
    
    if (x * x) + (y * y) <= 1:
        return 1

    return 0


iterations = 1000000 * 1000

rdd_sim = sc.parallelize(range(0, iterations))

rdd_sim = rdd_sim.map(lambda iteration: (throw_dart(iteration), iteration))

hits = rdd_sim.countByKey()[1]
pi = (4 * hits) / iterations
print(iterations)
print(pi)
