
# udf-databricks-cluster-multiprocessing
Run user defined function (UDF) on databricks clusting in multiprocessing mode

1. Import https://github.com/daviabdallah/databricks-utils/blob/main/udf-databricks-cluster-multiprocessing/udf-databricks-cluster-multiprocessing.py to a databricks notebook
2. Change function def throw_dart(iteraction) - line 5 -to your desired function (optional)
3. If function throw_dart(iteraction) was changed, change map rdd_sim.map(lambda iteration: (throw_dart(iteration), iteration)) - line 19 - required if function signature of throw_dart(iteraction) changed
4. Run notebook
