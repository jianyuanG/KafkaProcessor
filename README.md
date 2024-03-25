# KafkaProcessor

Go to your Kafka folder -> Go to config folder -> Edit the server.properties -> Uncomment and change the advertised.listeners=PLAINTEXT://127.0.0.1:9092 -> Save



Run these commands below:

./bin/zookeeper-server-start.sh config/zookeeper.properties

./bin/kafka-server-start.sh config/server.properties

./bin/kafka-console-consumer.sh  --topic Weather-data --bootstrap-server localhost:9092 // Realtime stream processor Q2

./bin/kafka-console-consumer.sh  --topic Weather-data-2 --bootstrap-server localhost:9092 // Realtime stream processor Q3 

When you are done:
./bin/zookeeper-server-stop.sh config/zookeeper.properties

./bin/kafka-server-stop.sh config/server.properties
