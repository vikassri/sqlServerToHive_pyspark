source ./env_files/env.sh
# spark-submit --jars spark-cassandra-connector.jar cassandraToHive.py <config> <keyspace>
spark-submit --jars spark-cassandra-connector.jar cassandraToHive.py configs/config.ini test