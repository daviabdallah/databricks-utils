# start-notebook-in-specific-cluster

Start notebook in specific cluster (different from current cluster)

1. Set parameter cluster_config. Pass json for creating new cluster or get cluster id by running spark.conf.get("spark.databricks.clusterUsageTags.clusterId") at the desired cluster 
2. Update "Notebook path to run"
3. Run notebook
