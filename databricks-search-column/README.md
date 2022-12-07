
# databricks-search-column-tool
search column tool (all databases or selected databases) for databricks

1. Create a new Databricks Notebook for your databricks-search-column-tool
2. Add to cells
3. Cell 1, add the contents of create_allColumns_tempView.py
4. Edit Cell 1, line 30: db_list = ['raw'] # [x[0] for x in spark.sql("SHOW DATABASES").rdd.collect()]
  4.1 List desired databases or [x[0] for x in spark.sql("SHOW DATABASES").rdd.collect()] for all databases in the catalog
5. Cell 2, add the contents of allColumns.sql (edit where clause for desired column name, database, etc
  5.1 The view allColumns contains database (string), tableName (string), and columnName (string), use the SQL language for desired query crieria
6. Run the databricks-search-column-tool notebook
