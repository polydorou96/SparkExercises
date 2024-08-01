from pathlib import Path

from dao.load_data import count_number_of_applications, read_schema_into_df, spark
from domain.define_schemas import loan_schema, application_schema, marketing_source_schema


class ExerciseCode:
    def __init__(self, application_df):
        self.application_df = application_df

    def number_of_applications(self):
        return self.application_df.count()


if __name__ == '__main__':
    application_df = read_schema_into_df(spark_session=spark,
                                         schema=application_schema,
                                         root_directory=Path("input/spark-interview/applications_parquet"))
    exercise = ExerciseCode(application_df)
    no_app = exercise.number_of_applications()
    a=True