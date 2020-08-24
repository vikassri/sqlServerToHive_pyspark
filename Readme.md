
## IMPORT DATABASE/TABLES FROM SQLSERVER TO HIVE USING PYSPARK
---

This project is to import the database with table and partitions from MS Sql using pyspark. Code is written in pyspark with mssql Spark Connector Jar. It will create hive tables in provided format(i.e parquet, orc etc)

### REQUIREMENT
* Import data from MS SQL to Hive 
* Customise Source database Name and Hive Database
* Insert Count Validation
* Create table and Append data into table 
* Load data in Dynamic and Static Partitions


### **Components:**

* Config File (Json Format)
* Environment file
* Pyspark Code
* Wrapper Script


#### ConfigFile
Config file will have details regarding database/host and port etc
```json
{
  "last_update_by": "Vikas Srivastava",
  "last_update_date": "2020-08-23",
  "hostname": "localhost",
  "port": 3306,
  "icol": "id",
  "database": ["customer_mysql", "customer_my"],
  "table_list": ["trips", "trips1"],
  "partition": "None"
}
```


#### Environment file
This will have secret configs and exported on runtime only.
```bash
export user=root
export password=hadoop
export hive_db=customer
export target_path=/tmp/tables/mysql
# format orc, parquet
export format=parquet
# mode can be ignore, overwrite, append
export mode=append
```

#### Wrapper Script 
we need to execute this script only
```bash
source ./env_files/env.sh
# spark-submit --jars mssql-jdbc-8.4.0.jre8.jar cassandraToHive.py <config> 
spark-submit --jars mssql-jdbc-8.4.0.jre8.jar sqlServerToHive.py configs/config.ini
```

### Steps 

**You need to clone the repo**
```bash
git clone https://github.com/vikassri/sqlServerToHive_pyspark.git
cd sqlServerToHive_pyspark
```

**update the config files**

```bash
[<source db 1>]
hostname=<hostname>
port=<port>
table_list=<table1>
partition=None

[<source db 2>]
hostname=<hostname>
port=<port>
table_list=<table1>
partition=None
```

**Update the Environment file**

```bash
export user=<mssql_user>
export password=<mssql_password>
export target_path=<hive_path_to_store_table>
export hive_db=<hive database>
export target_path=<target_path>
# format orc, parquet
export format=parquet
# mode can be ignore, overwrite, append
export mode=append
```
Finally, Execute the script with wrapper
```bash
# write the logs in console
sh wrapper.sh dbfile.list

# write the logs in nohup files
nohup sh wrapper.sh dbfile.list &
```

Once its completed successfully you can check the data in hive, database name will be keyspace and tables will be same named as cassandra tables.

Let me know if any issues.
