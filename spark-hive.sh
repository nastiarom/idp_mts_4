JUMP_NODE=$1
NAME_NODE=$2
USER=$3

ssh "$USER@$JUMP_NODE" << EOF
    sudo -i -u hadoop << EOL
        sudo apt install -y python3-venv python3-pip

        wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
        tar -xzvf spark-3.5.3-bin-hadoop3.tgz

        export HADOOP_CONF_DIR="/home/hadoop/hadoop-3.4.0/etc/hadoop"
        export HIVE_HOME="/home/hadoop/apache-hive-4.0.1-bin"
        export HIVE_CONF_DIR=\$HIVE_HOME/conf
        export HIVE_AUX_JARS_PATH=\$HIVE_HOME/lib/*
        export PATH=\$PATH:\$HIVE_HOME/bin
        export SPARK_LOCAL_IP="$JUMP_NODE"
        export SPARK_DIST_CLASSPATH="/home/hadoop/spark-3.5.3-bin-hadoop3/jars/*:/home/hadoop/hadoop-3.4.0/etc/hadoop:/home/hadoop/hadoop-3.4.0/share/hadoop/common/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/common/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/*:/home/hadoop/hadoop-3.4.0/share/hadoop/mapreduce/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/lib/*"
        
        cd spark-3.5.3-bin-hadoop3/
        export SPARK_HOME=\$(pwd)
        export PYTHONPATH=\$(echo "\$SPARK_HOME/python/lib/*.zip" | tr ' ' ':'):\$PYTHONPATH
        export PATH=\$SPARK_HOME/bin:\$PATH 
        
        cd ../
        python3 -m venv venv
        source venv/bin/activate
        
        hive --hiveconf hive.server2.enable.doAs=false --hiveconf hive.security.authorization.enable=false --service metastore &
        
        pip install --upgrade pip
        pip install ipython
        pip install onetl[files]

        ipython -c "
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from onetl.connection import SparkHDFS
from onetl.connection import Hive
from onetl.file import FileDFReader
from onetl.file.format import CSV
from onetl.db import DBWriter

spark = SparkSession.builder \
    .master('yarn') \
    .appName('spark-with-yarn') \
    .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
    .config('spark.hive.metastore.uris', 'thrift://$JUMP_NODE:5432') \
    .enableHiveSupport() \
    .getOrCreate()

hdfs = SparkHDFS(host='$NAME_NODE', port=9000, spark=spark, cluster='test')
reader = FileDFReader(connection=hdfs, format=CSV(delimiter=',', header=True), source_path='/input')
df = reader.run(['for_spark.csv'])

df = df.withColumn(df.columns[0], F.upper(F.col(df.columns[0])))

df.write \
    .mode('overwrite') \
    .saveAsTable('test.spark_partitions')

hive = Hive(spark=spark, cluster='test')
df_check = spark.sql('SELECT * FROM test.spark_partitions')
df_check.show()
spark.stop()
"
EOL
EOF