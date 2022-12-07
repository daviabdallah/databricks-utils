# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### It in turn executes the following notebooks in order because of dependencies

# COMMAND ----------

# Clear old widgets
dbutils.widgets.removeAll()
# Run <NOTEBOOK1 PATH> in sequential order
dbutils.notebook.run('<NOTEBOOK1 PATH>', 0)

# COMMAND ----------

import concurrent.futures

notebooks_to_run = []

# Run <NOTEBOOK2 PATH> and <NOTEBOOK3 PATH> in parallel
notebooks_to_run.append({"path": '<NOTEBOOK2 PATH>',"timeout": 0,"args": {}})
notebooks_to_run.append({"path": '<NOTEBOOK3 PATH>',"timeout": 0,"args": {}})

def run_with_retry(path, timeout=0, args = {}, max_retries=2):
  num_retries = 0
  return_dict = {}
  return_dict['path'] = path
  return_dict['timeout'] = timeout
  return_dict['arguments'] = args

  while True:
    try:
      print(f' Starting Notebook: {path} | Arguments: {args} | Timeout: {timeout}s')
      return_dict['notebook_output'] = dbutils.notebook.run(path, timeout, args)
      return return_dict
    except Exception as e:
      if num_retries >= max_retries:
       raise e
      else:
        num_retries += 1
        print(f"Error running...will run attempt {num_retries}")
  
with concurrent.futures.ThreadPoolExecutor(max_workers=50) as e:
    future_control = [e.submit(run_with_retry,notebook["path"],notebook["timeout"], notebook["args"], 1) for notebook in notebooks_to_run]
      
    for future in concurrent.futures.as_completed(future_control):      
      print('----------------------------------------------')
      path = 'tbd' #future_control[future]
      try:
          result = future.result()
      except Exception as exc:
          print('%s generated an exception: %s' % (path, exc))
          raise type(exc)('%s generated an exception: %s' % (path, exc))
      else:
          path = result['path']
          notebook_output = result['notebook_output']
          print('Successfully executed notebook: "%s"' % (path))
          print('  Result was: "%s"' % (notebook_output))

