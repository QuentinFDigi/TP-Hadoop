#!/bin/bash

# Nettoyage de la sortie hdfs
hdfs dfs -rm -r -f output/job_dance_energy

rm /datavolume1/**/*.log

# Initialisatio de Hbase
python3 /datavolume1/TP/hbase.py

# Lancement du job Hadoop Streaming
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -file /datavolume1/TP/mapper.py -mapper "python3 mapper.py" -file /datavolume1/TP/reducer.py -reducer "python3 reducer.py" -input input/Spotify_Most_Streamed_Songs.csv -output output/job_dance_energy

# Affichage du résultat
hdfs dfs -tail output/job_dance_energy/part-00000

# Visualisation depuis HBase
python3 /datavolume1/TP/hbase_to_pdf.py