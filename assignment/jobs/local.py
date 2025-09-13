from assignment.objectives.stock_stats import StockStats
from assignment.utils.results_handlers import PrintResultsHandler


if __name__ == '__main__':
    stocks_stats = StockStats(
        data_path='stocks_data.csv',
        results_handler=PrintResultsHandler()
    )
    stocks_stats.run()
