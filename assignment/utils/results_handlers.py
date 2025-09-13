import posixpath

from pyspark.sql import DataFrame as SparkDataFrame


class BaseResultsHandler:

    def handle(self, df: SparkDataFrame, name: str) -> None:
        raise NotImplementedError('Subclasses must implement handle method')


class PrintResultsHandler(BaseResultsHandler):

    def handle(self, df: SparkDataFrame, name: str) -> None:
        print(f'Results for {name}:')
        df.show(truncate=False)


class S3ParquetResultsHandler(BaseResultsHandler):

    def __init__(self, output_base_path: str):
        self.output_base_path = output_base_path

    def handle(self, df: SparkDataFrame, name: str) -> None:
        output_path = posixpath.join(self.output_base_path, name)
        df.coalesce(1).write.mode('overwrite').parquet(output_path)
        print(f'Results saved to {output_path}')