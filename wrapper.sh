source ./env_files/env.sh
# spark-submit --jars mssql-jdbc-8.4.0.jre8.jar sqlServerToHive.py <config> <database>
for database in `cat $1`
do
  spark-submit --jars mssql-jdbc-8.4.0.jre8.jar sqlServerToHive.py configs/config.ini $database
  sleep 5
done
