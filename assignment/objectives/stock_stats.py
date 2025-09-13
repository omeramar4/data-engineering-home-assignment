from pyspark.sql import SparkSession
from pyspark.sql import Window, functions as f, DataFrame as SparkDataFrame
from pyspark.sql.types import DateType

from assignment.utils.results_handlers import BaseResultsHandler, S3ParquetResultsHandler


class StockStats:

    NUM_OF_TRADING_DAYS_IN_A_YEAR = 252

    def __init__(self, data_path: str, results_handler: BaseResultsHandler = None):
        self.data_path = data_path
        self.results_handler = results_handler

    @property
    def spark(self) -> SparkSession:
        return SparkSession.builder.getOrCreate()

    def run(self):
        df = self._load_data()
        df_with_daily_return = self._get_daily_return(df)

        # Objective 1: Average daily return
        average_daily_return = self._get_average_daily_return(df_with_daily_return)
        self.results_handler.handle(average_daily_return, "objective_1_average_daily_return")

        # Objective 2: Highest average trade worth
        highest_average_trade_worth = self._get_highest_average_trade_worth(df)
        self.results_handler.handle(highest_average_trade_worth, "objective_2_highest_trade_worth")

        # Objective 3: Most volatile stock
        most_volatile_stock = self._get_most_volatile_stock(df_with_daily_return)
        self.results_handler.handle(most_volatile_stock, "objective_3_most_volatile_stock")

        # Objective 4: Top three 30-day returns
        top_three_30d_returns = self._get_top_three_30d_returns(df)
        self.results_handler.handle(top_three_30d_returns, "objective_4_top_30d_returns")

    def _load_data(self):
        return self.spark.read.csv(self.data_path, header=True, inferSchema=True)

    @staticmethod
    def _get_daily_return(df: SparkDataFrame) -> SparkDataFrame:
        window_spec = Window.partitionBy('ticker').orderBy('Date')
        return (
            df
            .withColumn('prev_close', f.lag('close').over(window_spec))
            .withColumn('daily_return', (f.col('close') - f.col('prev_close')) / f.col('prev_close'))
            .drop('prev_close')
        )

    @staticmethod
    def _get_average_daily_return(df: SparkDataFrame) -> SparkDataFrame:
        return (
            df
            .groupBy('Date')
            .agg(f.avg('daily_return').alias('average_return'))
            .withColumn('date', f.col('Date').cast(DateType()))
            .select('date', 'average_return')
        )

    @staticmethod
    def _get_highest_average_trade_worth(df: SparkDataFrame) -> SparkDataFrame:
        return (
            df
            .withColumn('trade_worth', f.col('close') * f.col('volume'))
            .groupBy('ticker')
            .agg(f.avg('trade_worth').alias('value'))
            .orderBy(f.desc('value'))
            .limit(1)
        )

    def _get_most_volatile_stock(self, df: SparkDataFrame) -> SparkDataFrame:
        return (
            df
            .groupBy('ticker')
            .agg(
                (
                    f.stddev('daily_return') * f.sqrt(f.lit(self.NUM_OF_TRADING_DAYS_IN_A_YEAR))
                ).alias('standard_deviation')
            )
            .orderBy(f.desc('standard_deviation'))
            .limit(1)
        )

    @staticmethod
    def _get_top_three_30d_returns(df: SparkDataFrame) -> SparkDataFrame:
        window_spec = Window.partitionBy('ticker').orderBy('Date')

        return (
            df
            .withColumn('close_30d_ago', f.lag('close', 30).over(window_spec))
            .withColumn('return_30d', ((f.col('close') - f.col('close_30d_ago')) / f.col('close_30d_ago')))
            .withColumn('date', f.col('Date').cast(DateType()))
            .select('ticker', 'date', 'return_30d')
            .orderBy(f.desc('return_30d'))
            .drop('return_30d')
            .limit(3)
        )
