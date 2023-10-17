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

rdd_sim_spark = rdd_sim.map(lambda iteration: (throw_dart(iteration), iteration))

hits = rdd_sim_spark.countByKey()[1]
pi = (4 * hits) / iterations
print(iterations)
print(pi)
