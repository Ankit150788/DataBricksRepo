import dlt


# Define the raw data table
@dlt.table(comment="Raw sales data from CSV stored in S3")
def raw_sales_data():
    return spark.read.csv("s3://databricks-workspace-stack-99dda-ankit-bucket/mumbai-prod/sample_data/train.csv", header=True, inferSchema=True)