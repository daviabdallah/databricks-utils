# Databricks notebook source
import concurrent.futures

def run_with_retry(path, timeout=0, args = {}, max_retries=0):
    num_retries = 0
    return_dict = {}
    return_dict['path'] = path
    return_dict['timeout'] = timeout
    return_dict['arguments'] = args
    
    while True:
        try:
            print(f'Starting Notebook: {path} | Arguments: {args} | Timeout: {timeout}s \n')
            return_dict['notebook_output'] = dbutils.notebook.run(path, timeout, args)
            return return_dict
        except Exception as e:
            if num_retries >= max_retries:
                raise e
            else:
                num_retries += 1
                print(f"Error running...will run attempt {num_retries}")

# COMMAND ----------

notebooks_to_run = []

notebooks_to_run.append({"path": './parallel_run_1',"timeout": 0,"args": {"dv_loadts": "2021-06-04T10:23:53.31", "dv_loadts1": "2022-06-04T10:23:53.31"}})
notebooks_to_run.append({"path": './parallel_run_2',"timeout": 0,"args": {"dv_loadts": "2021-07-04T10:23:53.31", "dv_loadts1": "2022-06-04T10:23:53.31"}})


# COMMAND ----------

with concurrent.futures.ThreadPoolExecutor(max_workers=50) as e:
    future_control = [e.submit(run_with_retry,notebook["path"],notebook["timeout"], notebook["args"], 1) for notebook in notebooks_to_run]
    
    for future in concurrent.futures.as_completed(future_control):
        print('----------------------------------------------')
        path = 'tbd' #future_control[future]
        try:
            result = future.result()
        except Exception as exc:
            print('%s generated an exception: %s \n' % (path, exc))
        else:
            path = result['path']
            notebook_output = result['notebook_output']
            print('Successfully executed notebook: "%s" \n' % (path))
            print('  Result was: "%s" \n' % (notebook_output))
