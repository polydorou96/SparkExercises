from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def get_show_string(df: DataFrame, n: int = 20, truncate: bool = True, vertical: bool = False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 20, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)


def initiate_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_parquet_file_into_spark_df(spark_session: SparkSession, root_directory: Path) -> DataFrame:
    parquet_files = list(root_directory.glob("*.parquet"))
    return spark_session.read.option("inferSchema", "true").parquet(*[str(file) for file in parquet_files])
