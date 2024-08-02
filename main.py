from pathlib import Path

from dao.exercises import count_number_of_applications, get_average_profit, \
    get_popular_marketing_sources, get_profit_percentage_from_each_marketing_source
from dao.logger import setup_logger
from utils.spark_utils import get_show_string, initiate_spark_session, read_parquet_file_into_spark_df


def main():
    logger = setup_logger()
    logger.info("Initiating spark session...")
    spark = initiate_spark_session(app_name='LoanApplicationsAnalysis')

    logger.info("Loading parquet files in spark dataframes...")
    loans = read_parquet_file_into_spark_df(spark_session=spark,
                                            root_directory=Path("input/spark-interview/loans_parquet"))
    applications = read_parquet_file_into_spark_df(spark_session=spark,
                                                   root_directory=Path("input/spark-interview/applications_parquet"))
    marketing_sources = read_parquet_file_into_spark_df(spark_session=spark,
                                                        root_directory=Path("input/spark-interview/sources_parquet"))
    logger.info("Parquet files have been loaded successfully")

    number_of_applications = count_number_of_applications(applications=applications)
    logger.info("Exercise 1:")
    logger.info(f"We have received a total number of {number_of_applications} applications")
    average_profit = get_average_profit(applications=applications, loans=loans)
    logger.info("Exercise 2:")
    logger.info(f"The average profit of all applications is: £{average_profit}")
    top_two_marketing_sources = get_popular_marketing_sources(applications=applications, loans=loans,
                                                              marketing_sources=marketing_sources)
    logger.info("Exercise 3:")
    logger.info("These are the top 2 popular marketing sources:")
    logger.info(f"{get_show_string(df=top_two_marketing_sources)}")
    profit_percentage = get_profit_percentage_from_each_marketing_source(applications=applications,
                                                                         loans=loans,
                                                                         marketing_sources=marketing_sources)
    logger.info("Exercise 4:")
    logger.info("These are the percentage profits from each marketing source:")
    logger.info(f"{get_show_string(df=profit_percentage)}")


if __name__ == "__main__":
    main()
