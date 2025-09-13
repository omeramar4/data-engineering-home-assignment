import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from assignment.objectives.stock_stats import StockStats
from assignment.utils.results_handlers import S3ParquetResultsHandler


if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "DATA_PATH"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    stocks_stats = StockStats(
        data_path=args["DATA_PATH"],
        results_handler=S3ParquetResultsHandler(output_base_path=f"s3://{args['S3_BUCKET']}/results")
    )
    stocks_stats.run()
    job.commit()
