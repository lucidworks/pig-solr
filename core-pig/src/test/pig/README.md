Note, these Pig scripts can only be run in standalone mode, as we don't have a unit test framework for Pig.

To run:

1. Download Pig 0.10 from pig.apache.org and set it up per the instructions
2. Run first: $HADOOP_HOME/bin/hadoop fs -put ./hadoop/build/hadoop-lws-job.jar /hadoop/hadoop-lws-job.jar
1. Get the Pig Mix tool from Duke: https://www.cs.duke.edu/starfish/mr-apps/pigmix.tar.gz
1. Run Hadoop in Single Node mode: hadoop.apache.org/docs/r1.2.1/single_node_setup.html
1. Build the pigmix tool by running ant in the directory
1. $PIG_MIX_HOME/scripts/generate_data.sh 5 50 hdfs:///path/to/data/gen-data
4. You will need a Collection called "test-pig" in LWS:  curl -X POST -H Content-type:application/json -d '{"name":"test-pig"}' http://localhost:8888/api/collections
3. $PIG_HOME/bin/pig -Dpig.additional.jars=/Users/grantingersoll/projects/lucid/hadoop-lws/hadoop/build/hadoop-lws-job.jar:/Users/grantingersoll/Downloads/pigmix/pigperf.jar:/Users/grantingersoll/projects/lucid/hadoop-lws/hadoop/lib/ivy/compile/slf4j-api-1.7.2.jar  [-p OTHER_PARAMS] -p input=/users/grantingersoll/gen-data.dat /path/to/pig/file

