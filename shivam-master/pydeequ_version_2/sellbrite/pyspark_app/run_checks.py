#from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp,lit
#from configparser import ConfigParser
#from pydeequ.checks import *
from datetime import datetime
#from pyspark.sql import Row, DataFrame
#import pydeequ, pyspark
#from pyspark.sql.types import *
from pydeequ import Check,CheckLevel
from pydeequ.verification import *
from pydeequ.analyzers import *
import sys
#import calendar, time
from functools import reduce
from pyspark.sql.utils import AnalysisException

class Pydeequ_Analyzer:

    # read datasets
    def get_datasets(self, path, file_format):
        if file_format == "csv":
            dataframe = spark.read.format('csv').options(header=True, inferSchema=True).load(path)
        elif file_format == "parquet":
            dataframe = spark.read.parquet(path)
        return dataframe

    # get dataset and analyze metrics
    def get_analyzer(self,sections):
        file_format = Parser[Env][sections]['raw_file_format']
        raw_path = Parser[Env][sections]['raw_s3_path']
        dataframe = self.get_datasets(raw_path, file_format)
        
        metrics = Parser[Env][sections]['metrics'].split('|')
        Analyzer = AnalysisRunner(spark).onData(dataframe)
        
        Analyzer_Report = lambda rules: Analyzer.addAnalyzer(eval(rules))        
        Analyzer_Report_Dataframe = AnalyzerContext.successMetricsAsDataFrame(spark, list(map(Analyzer_Report, metrics))[0].run())
        Analyzer_Report_Dataframe = Analyzer_Report_Dataframe.withColumn('ingestion_tsp', lit(current_timestamp())).withColumn('source_table', lit(sections))
        return Analyzer_Report_Dataframe


    def collect_analysis_report_datasets(self, Analyzer_Report_list):
        print("Displaying Analyzers")
        Union_Analyzer_Report = reduce(DataFrame.union, Analyzer_Report_list)
        
        print("Saving the data to the Given Location")
        today = datetime.now()
        Analyzer_Report_path = sys.argv[3] + 'analyzer//' + today.strftime('%Y-%m-%d')
        Union_Analyzer_Report.coalesce(1).write.parquet(f"{Analyzer_Report_path}", mode='overwrite')
        Union_Analyzer_Report.show(truncate=False)
        return Union_Analyzer_Report



class Pydeequ_Check_Verification:

    now = datetime.now()

    # get dataset
    def check_fileformat_generate_dataset(self, sections):
        clean_path = str(Parser[Env][sections]['clean_source']) + "year={:4d}/month={:02d}/day={:02d}".format(self.now.year, self.now.month, self.now.day) + "/**"
        print("##############", clean_path)
        file_format = Parser[Env][sections]['clean_file_format']

        try:
            if file_format == "csv":
                dataframe = spark.read.options(header=True, inferSchema=True).csv(clean_path)
                check_result_dataframe = self.verify_checks_on_datasets(dataframe, sections)
            elif file_format == "parquet":
                dataframe = spark.read.parquet(clean_path)
                check_result_dataframe = self.verify_checks_on_datasets(dataframe, sections)
            return check_result_dataframe
        except AnalysisException as e:
            print(f"{sections} file not found")
            return None


    # creating checks string from analyzer instances
    def createcheckstringfromanalyzer(self,analyzer_instance):
        new_checks = ""
        if analyzer_instance[2] == 'hasNumberOfDistinctValues':
            new_checks += analyzer_instance[2] + "('" + analyzer_instance[0] + "'" + ",lambda x: x==" + str(analyzer_instance[1]) + ",maxBins=10,binningUdf=None)|"
        elif analyzer_instance[2] == 'hasSize':
            new_checks += analyzer_instance[2] + "(lambda x: x==" + str(analyzer_instance[1]) + ")|"
        else:
            new_checks += analyzer_instance[2] + "('" + analyzer_instance[0] + "'" + ",lambda x: x==" + str(analyzer_instance[1]) + ")|"
        return new_checks


    # verifying checks
    def verify_checks_on_datasets(self, tables, sections):

        filtered_sectional_dataframe = Union_Analyzer_Report.filter(Union_Analyzer_Report.source_table == sections)
        filtered_analyzer_rdd = filtered_sectional_dataframe.select("instance", "value", "name").rdd.map(
            lambda analyzer_row: (analyzer_row[0], analyzer_row[1], checks_analyzer_mapper[analyzer_row[2]]) if analyzer_row[2] in checks_analyzer_mapper.keys()
            else (analyzer_row[0], analyzer_row[1], analyzer_row[2]))

        modified_attribute = Parser[Env][sections]['modified_attributes']
        if modified_attribute != '':
            modified_attribute_dictionary = modified_attribute.asDict()
            filtered_analyzer_rdd = filtered_analyzer_rdd.map(lambda analyzer_row: (modified_attribute_dictionary[analyzer_row[0]], analyzer_row[1], analyzer_row[2]) if analyzer_row[0] in modified_attribute_dictionary.keys() else (analyzer_row[0], analyzer_row[1], analyzer_row[2]))

        analyzer_dataframe_checks = "".join(map(self.createcheckstringfromanalyzer, filtered_analyzer_rdd.collect())).split('|')[:-1]
        analyzer_dataframe_checks = ".".join(analyzer_dataframe_checks)

        verification_checks = Parser[Env][sections]['checks'].split('|')
        verification_checks.insert(0, "check")
        checks = ".".join(verification_checks) + '.' + analyzer_dataframe_checks

        check = Check(spark, CheckLevel.Error, Parser[Env][sections]['check_name'])
        Verifying_Checks = (VerificationSuite(spark)
                       .onData(tables)
                       .addCheck(eval(checks))
                       .run())
        Check_Reports = VerificationResult.checkResultsAsDataFrame(spark, Verifying_Checks)
        Check_Reports_Dataframe = (Check_Reports.withColumn('dataset_name', lit(sections)).withColumn('check_run_tsp',lit(current_timestamp())))

        return Check_Reports_Dataframe


    def collect_check_report_datasets(self, Check_Report_list):
        print("Displaying Checks")
        Union_Check_Report = reduce(DataFrame.union, Check_Report_list)

        print("Saving the data to the Given Location")
        today = datetime.now()
        Check_Report_path = sys.argv[3] + 'checks//' + today.strftime('%Y-%m-%d')
        Union_Check_Report.coalesce(1).write.parquet(f"{Check_Report_path}", mode='overwrite')
        Union_Check_Report.show(truncate=False)


if __name__ == "__main__":
    
    Env = sys.argv[1]
    config_path = sys.argv[2]
    spark = SparkSession.builder.appName("Pydeequ verifying checks").getOrCreate()
    print("Initialized SparkSession")
    # read properties of config
    print("############", config_path)
    config_json = spark.read.option("multiline", "true").json(config_path)
    Parser = list(map(lambda row: row.asDict(),config_json.collect()))[0]
    sections = Parser[Env]['dataset']

    # analyzer class
    Pydeequ_Analyzer_object = Pydeequ_Analyzer()
    Analyzer_Report_list = list(map(Pydeequ_Analyzer_object.get_analyzer, sections))
    Union_Analyzer_Report = Pydeequ_Analyzer_object.collect_analysis_report_datasets(Analyzer_Report_list)

    # check class
    checks_analyzer_mapper = {'CountDistinct': 'hasNumberOfDistinctValues', 'ApproxCountDistinct': 'hasApproxCountDistinct',
                'Maximum': 'hasMax', 'Completeness': 'hasCompleteness', 'Size': 'hasSize'}

    print("now starting checks")
    Pydeequ_Check_Verification_object = Pydeequ_Check_Verification()
    Check_Reports_list = list(map(Pydeequ_Check_Verification_object.check_fileformat_generate_dataset, sections))
    Check_Report_list = list(filter(None, Check_Reports_list))
    if Check_Report_list:
        Pydeequ_Check_Verification_object.collect_check_report_datasets(Check_Report_list)
    else:
        print("Dataset File Not Found!")
        
    spark.stop()
