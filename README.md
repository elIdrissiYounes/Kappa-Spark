Kappa-Spark

Inspired from two main courses on Udemy and Pluralsight.

Ahmed El KILANI: https://www.pluralsight.com/courses/spark-kafka-cassandra-applying-lambda-architecture
Frank Kane: https://www.udemy.com/taming-big-data-with-spark-streaming-hands-on/ 

The project can be run on IntellJ with adding the proper scala versions 2_11.
This a maven based project, a simple mvn package would build the project and create 
a Spark Streaming jar that can be run on different spark configurations (Single node, Cluster mode)
 
------------------------------------------------------------------------------------------------------
Other tools should be set up to run the application a Kafka server with zookeper
Elasticsearch and Kibana, please refer to the Elastic Stack website to set these two latter platforms

For the kafka message broker, a topic should be created and changed in the config file of this application, as long as the IP addresses of all different concerned systems.
