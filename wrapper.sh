source ./env_files/env.sh
# spark-submit --jars mssql-jdbc-8.4.0.jre8.jar sqlServerToHive.py <config> <database>
spark-submit --jars mssql-jdbc-8.4.0.jre8.jar sqlServerToHive.py configs/config.json
