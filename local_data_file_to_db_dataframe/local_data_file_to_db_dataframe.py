import pandas as pd

with open("/Workspace/Users/your_user_name/your_filepath") as local_data_file:
    pdf = pd.read_csv(local_data_file)

df = spark.createDataFrame(pdf)

df.createOrReplaceTempView("local_data")
