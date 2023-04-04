# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text('dv_loadts', '2021-06-04T10:23:53.31', 'dv_loadts')    # where is the data
dbutils.widgets.text('dv_loadts1', '2021-06-04T10:23:53.31', 'dv_loadts')    # where is the data
dv_loadts = getArgument('dv_loadts')
dv_loadts1 = getArgument('dv_loadts1')

import time
print("parallel_run_2 started")
print(dv_loadts)
print(dv_loadts1)
time.sleep(1)
if True:
    raise Exception("parallel_run_2 error")
print("parallel_run_2 complete succesuflly")
