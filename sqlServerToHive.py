# **********************************************************************************************************************************
# Auther       : Vikas Srivastava
# Description  : This utility will connect to sqlServer cluster using pyspark and extract the data table wise and write to your
#                destination path (hdfs/s3) in parquet/CSV format. In order to acheive the desired output you need to call
#                this pyspark script from shell script or any script specifying below variables.
# How to Call  : spark-submit --jars <mssql-connector.jar> <sqlServerToHive.py> <config.ini path> <database>
# Pre-Requiste : Below variables with the same name mentioned below to be exported prior to call the script.
#                database, user, password, query, target_path, op_format(Output format should be 'parquet' or
#                'csv'), op_mode(Output mode should be 'overwrite' or 'append') and delimiter(if op_format is csv)
# **********************************************************************************************************************************
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import configparser
import os
import sys


def main():
    config_file = sys.argv[1].lower()
    section = sys.argv[2].lower()
    if config_file is None:
        sys.exit(1)
    else:
        spark_load(config_file, section)


def log(type, msg):
    print("{}:{}: {}".format(str(datetime.now()), type.upper(), msg))


def spark_load(config_file, section):
    success_file = '{}_success_{}.list'.format(str(current_date()), database)
    s_file = open(success_file, 'a')
    appName = "PySpark SqlServer query Load"
    master = "local"
    spark = SparkSession.builder.config("spark.sql.parquet.writeLegacyFormat", "true").appName(
        appName).enableHiveSupport().master(master).getOrCreate()
    config = configparser.ConfigParser()
    config.read(config_file)
    sections = config.sections()
    database = section
    hive_db = os.environ['hive_db']
    server = config[section]['hostname']
    port = config[section]['port']
    table_list = config[section]['table_list']
    partition = config[section]['partition']
    user = os.environ['user']
    password = os.environ['password']
    target_path = os.environ['target_path']
    op_format = os.environ['format']
    op_mode = os.environ['mode']
    log("info", "pyspark script to extract data from sqlServer server is starting. Time")
    print("-----------------------------------------------------------------------------------------------------------------")
    print("Server Name      : " + server)
    print("Port Number      : " + port)
    print("User Name        : " + user)
    print("Source database  : " + database)
    print("hive database    : " + hive_db)
    print("Destination path : " + target_path)
    print("Output Format    : " + op_format)
    print("Mode of Output   : " + op_mode)
    print("Table List       : " + table_list)
    print("Incr. Column     : " + icol)
    print("-----------------------------------------------------------------------------------------------------------------")

    for table in table_list.split(','):
        if table not in open(success_file).read():
            log("info", "**** Running for Table {} ***".format(table))
            mssqlDF = spark.read.format("jdbc").option("url", "jdbc:sqlserver://" + server + ":" + port + ";databaseName=" + database).option("dbtable", table).option(
                "user", user).option("password", password).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("encoding", "ascii").load()
            mssqlDF = mssqlDF.select(F.current_date().alias(
                'offload_date'), '*', F.lit(database).alias('offload_database'))
            max_id = mssqlDF.agg({icol: "max"}).collect()[0][0]
            mssqlDF = mssqlDF[mssqlDF[icol] > max_id]
            mssqlDF.printSchema()
            record_count = mssqlDF.count()
            log("info", "Query read completed and loaded into spark dataframe")
            log("info", "Starting load to datalake target path")
            log("info", "Record count is "+str(record_count))
            log("info", "Checking if table already exists")
            current_count = 0
            if spark._jsparkSession.catalog().tableExists(hive_db, table):
                log("info", "Table already exists")
                log("Info", "Fetching the current count")
                current_count = spark.sql(
                    "select count(*) from {}.{}".format(hive_db, table)).collect()[0][0]
                log("info", "Current count: {}".format(current_count))
            else:
                log("info", "Table doesn't exists|| Creating now")
            try:
                target_path = "{}/{}/{}".format(target_path, hive_db, table)
                partition = "offload_database" if partition.lower() == "none" else partition
                log("info", "partition : {}".format(str(partition)))
                if op_format == "csv":
                    log("warn", "you need to be create csv table for {} with path {}".format(
                        table, target_path))
                spark.sql("CREATE database IF NOT EXISTS {}".format(hive_db))
                mssqlDF.write.saveAsTable(
                    hive_db+"."+table, format=op_format, mode=op_mode,  partitionBy=partition, path=target_path)
                s_file.write(table+"\n")
            except:
                log("error", "Loading failed, Running for next table !!")
            log("info", "dataframe loaded in {} format successfully into target path {}".format(
                op_format, target_path))
            log("info", "Data copyied for table {} successfully".format(table))
            updated_count = spark.sql(
                "select count(*) from {}.{}".format(hive_db, table)).collect()[0][0]
            log("info", "Total record: {}, Source count: {} and Inserted Record: {}".format(
                updated_count, record_count, updated_count-current_count))
        else:
            log("info", "Skipping as Already completed load for table:{}".format(table))
    success_file.close()


if __name__ == '__main__':
    main()
