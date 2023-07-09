# LondonAirbnbAnalysis

Commands: 

hdfs namenode -format
$HADOOP_HOME/sbin/start-all.sh

Add input to hdfs:
'hdfs dfs -mkdir /input'
'hdfs dfs -put input/London_Airbnb_Listings_March_2023.tsv /input'

Compile classes:
inside MapReduce folder: 
javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` {ClassName}.java
jar cf jar/{classname}.jar {ClassName}*

Run File
hadoop jar jar/{classname}.jar {ClassName} /input/London_Airbnb_Listings_March_2023.tsv /output/{classname}/

Remove Output
hdfs dfs -rm -r /output/{classname}/

Check output
hdfs dfs -ls /output/{classname}
hdfs dfs -cat /output/{classname}/part-r-00000 | head -50