cluster_config = '''
{
  "existing_cluster_id": "cluster id" # Pass json for creating new cluster or get cluster id by running spark.conf.get("spark.databricks.clusterUsageTags.clusterId") at the desired cluster 
}
'''

dbutils.notebook.run('Notebook path to run', 36000, _NotebookHandler__databricks_internal_cluster_spec=cluster_config)
