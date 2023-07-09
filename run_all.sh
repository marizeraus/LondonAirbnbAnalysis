hdfs namenode -format
$HADOOP_HOME/sbin/start-all.sh
hdfs dfs -mkdir /input
hdfs dfs -put input/London_Airbnb_Listings_March_2023.tsv /input

cd src/main/java/MapReduce

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` AcceptancePrice.java
jar cf jar/acceptanceprice.jar AcceptancePrice*
hadoop jar jar/acceptanceprice.jar AcceptancePrice /input/London_Airbnb_Listings_March_2023.tsv /output/acceptanceprice

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` Bigram.java
jar cf jar/bigram.jar Bigram*
hadoop jar jar/bigram.jar Bigram /input/London_Airbnb_Listings_March_2023.tsv /output/bigram

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` HostLocation.java
jar cf jar/hostlocation.jar HostLocation*
hadoop jar jar/hostlocation.jar HostLocation /input/London_Airbnb_Listings_March_2023.tsv /output/hostlocation


javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` HostSinceLocationPair.java
jar cf jar/hostsincelocationpair.jar HostSinceLocationPair*
hadoop jar jar/hostsincelocationpair.jar HostSinceLocationPair /input/London_Airbnb_Listings_March_2023.tsv /output/hostsincelocationpair

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` LatitudeLongitude.java
jar cf jar/latitudelongitude.jar LatitudeLongitude*
hadoop jar jar/latitudelongitude.jar LatitudeLongitude /input/London_Airbnb_Listings_March_2023.tsv /output/latitudelongitude

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` PropertyTypeLatLong.java
jar cf jar/propertytypelatlong.jar PropertyTypeLatLong*
hadoop jar jar/propertytypelatlong.jar PropertyTypeLatLong /input/London_Airbnb_Listings_March_2023.tsv /output/propertytypelatlong

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` Unigram.java
jar cf jar/unigram.jar Unigram*
hadoop jar jar/unigram.jar Unigram /input/London_Airbnb_Listings_March_2023.tsv /output/unigram