`create table host_location ( location String, count Integer) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";`

`LOAD DATA INPATH 'hdfs://localhost:{port}//output/host_location/ part-r-00000' INTO TABLE host_location;`

`create table bigram ( bigram String, count Integer) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"; `

`LOAD DATA INPATH 'hdfs://localhost:{port}//output/bigram/part-r-00000' INTO TABLE bigram;`

`create table unigram ( unigram String, count Integer) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";`

`LOAD DATA INPATH 'hdfs://localhost:{port}//output/unigram/part-r-00000' INTO TABLE unigram;`

`create table host_loc_since (host_location String, host_since Date) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t";`

`LOAD DATA INPATH 'hdfs://localhost:{port}//output/hostsincelocationpair/part-r-00000' INTO TABLE host_loc_since;`
