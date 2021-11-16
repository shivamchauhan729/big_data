from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from configparser import ConfigParser
import pydeequ
from pydeequ.checks import *
from pydeequ.verification import *
from datetime import datetime
from functools import reduce
import sys
from pyspark.sql.utils import AnalysisException


class Pydeequ_Check_Verification:
    now = datetime.now()

    def check_fileformat_generate_dataset(self, sections):
        path = str(Parser.get(sections, 'source')) + "year={:4d}/month={:02d}/day={:02d}".format(self.now.year,self.now.month,self.now.day) + "/**"
        print(path)
        file_format = Parser.get(sections,'file_format')

        try:
            if file_format == "csv":
                dataframe = spark.read.options(header=True, inferSchema=True).csv(path)
                check_result_dataframe = self.verify_checks_on_datasets(dataframe, sections)
            elif file_format == "parquet":
                dataframe = spark.read.parquet(path)
                check_result_dataframe = self.verify_checks_on_datasets(dataframe, sections)
            return check_result_dataframe
        except AnalysisException as e:
            print(f"{sections} file not found")
            return None



    def verify_checks_on_datasets(self, tables, sections):

        checker = Parser.get(sections, 'checks').split('|')
        check = Check(spark, CheckLevel.Error, Parser.get(sections,'check_name'))
        checker.insert(0, "check")
        checks = ".".join(checker)

        Verifying_Checks = (VerificationSuite(spark)
                            .onData(tables)
                            .addCheck(eval(checks))
                            .run())
        Check_Reports = VerificationResult.checkResultsAsDataFrame(spark, Verifying_Checks)
        Check_Reports_Dataframe = (Check_Reports.withColumn('dataset_name', lit(sections))
                                   .withColumn('check_run_tsp', lit(current_timestamp())))
        return Check_Reports_Dataframe


    def collect_check_report_datasets(self, Check_Report_list):
        print("Displaying Checks")
        Union_Check_Report = reduce(DataFrame.union, Check_Report_list)

        print("Saving the data to the Given Location")
        today = datetime.now()
        today.strftime('%Y%m%d')
        Check_Report_path = sys.argv[1] + today.strftime('%Y-%m-%d')
        Union_Check_Report.coalesce(1).write.parquet(f"{Check_Report_path}", mode='overwrite')
        Union_Check_Report.show()


if __name__ == "__main__":
    print("Reading Pydeequ Checks Properties")
    Parser = ConfigParser()
    Parser.read(r"checks.properties")
    sections = Parser.sections()
    spark = SparkSession.builder.appName("Pydeequ verifying checks").getOrCreate()
    print("Initialized SparkSession")

    Pydeequ_Check_Verification_object = Pydeequ_Check_Verification()
    Check_Reports_list = list(map(Pydeequ_Check_Verification_object.check_fileformat_generate_dataset, sections))
    Check_Report_list = list(filter(None, Check_Reports_list))

    if Check_Report_list:
        Pydeequ_Check_Verification_object.collect_check_report_datasets(Check_Report_list)
   
