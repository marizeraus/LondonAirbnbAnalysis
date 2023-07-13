cd src/main/java/MapReduce

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` Bigram.java
jar cf jar/bigram.jar Bigram*
hadoop jar jar/bigram.jar Bigram /input/London_Airbnb_Listings_March_2023.tsv /output/bigram

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` HostLocation.java
jar cf jar/hostlocation.jar HostLocation*
hadoop jar jar/hostlocation.jar HostLocation /input/London_Airbnb_Listings_March_2023.tsv /output/host_location

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` HostSinceLocationPair.java
jar cf jar/hostsincelocationpair.jar HostSinceLocationPair*
hadoop jar jar/hostsincelocationpair.jar HostSinceLocationPair /input/London_Airbnb_Listings_March_2023.tsv /output/hostsincelocationpair

javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` Unigram.java
jar cf jar/unigram.jar Unigram*
hadoop jar jar/unigram.jar Unigram /input/London_Airbnb_Listings_March_2023.tsv /output/unigram

cd ../../../..