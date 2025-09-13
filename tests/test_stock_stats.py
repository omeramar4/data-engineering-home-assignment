import unittest
from unittest.mock import patch
from datetime import date, timedelta
from pyspark.sql import SparkSession, functions as f, DataFrame as SparkDataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

from assignment.objectives.stock_stats import StockStats
from assignment.utils.results_handlers import BaseResultsHandler


class MockResultsHandler(BaseResultsHandler):

    def __init__(self):
        self.handled_results = {}

    def handle(self, df: SparkDataFrame, name: str) -> None:
        self.handled_results[name] = df


class TestStockStats(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName('StockStatsTest') \
            .config('spark.sql.execution.arrow.pyspark.enabled', 'false') \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.mock_handler = MockResultsHandler()
        self.stock_stats = StockStats('dummy_path', self.mock_handler)
        self.test_data = self._create_test_data()

    @patch.object(StockStats, '_load_data')
    def test_get_daily_return(self, mock_load_data):
        '''Test daily return calculation'''
        mock_load_data.return_value = self.test_data

        df_with_daily_return = self.stock_stats._get_daily_return(self.test_data)

        # Check that daily_return column is added
        self.assertIn('daily_return', df_with_daily_return.columns)

        # Collect results and verify first day has null return (no previous day)
        results = df_with_daily_return.filter(f.col('ticker') == 'AAPL').orderBy('Date').collect()
        self.assertIsNone(results[0]['daily_return'])

        # Check that second day has calculated return
        self.assertIsNotNone(results[1]['daily_return'])

    def test_get_average_daily_return(self):
        df_with_returns = self.stock_stats._get_daily_return(self.test_data)
        avg_returns = self.stock_stats._get_average_daily_return(df_with_returns)

        # Check that result has correct columns
        self.assertEqual(set(avg_returns.columns), {'date', 'average_return'})

        # Check that date column is DateType
        date_field = next(field for field in avg_returns.schema.fields if field.name == 'date')
        self.assertEqual(date_field.dataType, DateType())

        # Should have 40 rows (one per day)
        self.assertEqual(avg_returns.count(), 40)

    def test_get_highest_average_trade_worth(self):
        result = self.stock_stats._get_highest_average_trade_worth(self.test_data)

        # Should return exactly one row
        self.assertEqual(result.count(), 1)

        # Should have ticker and value columns
        self.assertEqual(set(result.columns), {'ticker', 'value'})

        # MSFT should have highest trade worth (highest volume and price)
        row = result.collect()[0]
        self.assertEqual(row['ticker'], 'MSFT')

    def test_get_most_volatile_stock(self):
        df_with_returns = self.stock_stats._get_daily_return(self.test_data)
        result = self.stock_stats._get_most_volatile_stock(df_with_returns)

        # Should return exactly one row
        self.assertEqual(result.count(), 1)

        # Should have ticker and standard_deviation columns
        self.assertEqual(set(result.columns), {'ticker', 'standard_deviation'})

        # GOOGL should be most volatile based on our test data
        row = result.collect()[0]
        self.assertEqual(row['ticker'], 'GOOGL')
        self.assertGreater(row['standard_deviation'], 0)

    def test_get_top_three_30d_returns(self):
        result = self.stock_stats._get_top_three_30d_returns(self.test_data)

        # Should return at most 3 rows
        self.assertLessEqual(result.count(), 3)

        # Should have ticker and date columns
        expected_columns = {'ticker', 'date'}
        self.assertEqual(set(result.columns), expected_columns)

        # Check that date column is DateType
        date_field = next(field for field in result.schema.fields if field.name == 'date')
        self.assertEqual(date_field.dataType, DateType())

    @patch.object(StockStats, '_load_data')
    def test_run_integration(self, mock_load_data):
        mock_load_data.return_value = self.test_data

        # Run the full pipeline
        self.stock_stats.run()

        # Verify all 4 objectives were handled
        self.assertEqual(len(self.mock_handler.handled_results), 4)

        handled_names = [name for name in self.mock_handler.handled_results.keys()]
        expected_names = [
            'objective_1_average_daily_return',
            'objective_2_highest_trade_worth',
            'objective_3_most_volatile_stock',
            'objective_4_top_30d_returns'
        ]

        for expected_name in expected_names:
            self.assertIn(expected_name, handled_names)

    def test_spark_property(self):
        spark_session = self.stock_stats.spark
        self.assertIsInstance(spark_session, SparkSession)

    def test_init(self):
        data_path = '/path/to/data.csv'
        handler = MockResultsHandler()

        stats = StockStats(data_path, handler)

        self.assertEqual(stats.data_path, data_path)
        self.assertEqual(stats.results_handler, handler)
        self.assertEqual(stats.NUM_OF_TRADING_DAYS_IN_A_YEAR, 252)

    def _create_test_data(self):
        schema = StructType([
            StructField('Date', StringType(), True),
            StructField('ticker', StringType(), True),
            StructField('close', DoubleType(), True),
            StructField('volume', IntegerType(), True)
        ])

        # Generate 40 days of data for 3 stocks to test 30-day returns
        base_date = date(2024, 1, 1)
        data = []

        # Stock A: steady growth
        for i in range(40):
            current_date = base_date + timedelta(days=i)
            data.append((current_date.strftime('%Y-%m-%d'), 'AAPL', 100.0 + i * 0.5, 1000000 + i * 10000))

        # Stock B: high volatility
        prices_b = [50.0, 55.0, 45.0, 60.0, 40.0, 65.0, 35.0, 70.0, 30.0, 75.0] * 4
        for i in range(40):
            current_date = base_date + timedelta(days=i)
            data.append((current_date.strftime('%Y-%m-%d'), 'GOOGL', prices_b[i], 500000 + i * 5000))

        # Stock C: low volatility, high volume
        for i in range(40):
            current_date = base_date + timedelta(days=i)
            data.append((current_date.strftime('%Y-%m-%d'), 'MSFT', 200.0 + i * 0.1, 2000000 + i * 20000))

        return self.spark.createDataFrame(data, schema)
