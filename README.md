
# Kafka Pipeline for Stock Market Anomalies

This repository contains a set of five Kafka-based Java applications that work together as a pipeline to process stock market data and send anomalies to RabbitMQ. The steps of the pipeline are as follows:

1. **Kafka Producer**: Reads data from Google Drive and sends it to a Kafka topic.
2. **Augment Data**: Augments the data by adding additional properties.
3. **MA200**: Calculates the 200-period Moving Average (MA200) to identify anomalies in the stock market candles.
4. **Isolation Forest**: Uses the Smile library's Isolation Forest class to detect anomalies in the stock market candles.
5. **Kafka Consumer**: Reads anomalies from the Kafka topic and sends them to RabbitMQ.

## How to Clone the Repository

To clone the repository, run the following command:

```bash
git clone https://github.com/your-username/kafka-pipeline.git
cd kafka-pipeline
```

## Setting Up Kafka (Using Kraft)

This project uses Kafka without Zookeeper (via Kraft mode). To get Kafka running:

1. **Download and Install Kafka**:
   - Follow the [Kafka Quickstart Guide](https://kafka.apache.org/quickstart) to download and set up Kafka.

2. **Start Kafka in Kraft Mode**:
   Kafka can run without Zookeeper in Kraft mode, which is simpler for local setups:
   ```bash
   bin/kafka-server-start.sh config/kraft-server.properties
   ```

3. **Create Topics**:
   Create the necessary Kafka topics for the producer, augmented data, anomalies, and consumer:
   ```bash
   bin/kafka-topics.sh --create --topic input-data --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
   bin/kafka-topics.sh --create --topic augmented --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
   bin/kafka-topics.sh --create --topic anomalies --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
   ```

## How to Compile and Run the Java Projects

Each application is located in its respective directory within the project. Follow these steps to compile and run each Kafka-based Java application:

1. **Build the Project Using Maven**:
   - Each application uses Maven for building and running. Navigate to each project folder (`kafka-consumer`, `kafka-producer`, etc.) and run the following command to compile the project:
   
   ```bash
   mvn clean install
   ```

2. **Running the Applications**:
   - Once the project is compiled, you can run the applications. You will need to run them in the following order:
     - **Producer**: Sends data from Google Drive to Kafka.
     - **Augment Data**: Processes and augments the data.
     - **MA200 and Isolation Forest**: These run in parallel and detect anomalies.
     - **Consumer**: Sends the anomalies to RabbitMQ.

   Use the following commands to run each application:

   ```bash
   mvn exec:java -Dexec.mainClass="org.aghersi.kafka.producer.KafkaProducerApp"
   mvn exec:java -Dexec.mainClass="org.aghersi.kafka.augmentData.AugmentDataApp"
   mvn exec:java -Dexec.mainClass="org.aghersi.kafka.streams.MA200App"
   mvn exec:java -Dexec.mainClass="org.aghersi.kafka.streams.IsolationForestApp"
   mvn exec:java -Dexec.mainClass="org.aghersi.kafka.consumer.KafkaConsumerApp"
   ```

   Make sure Kafka is running before starting the producer.

## RabbitMQ Integration

Once the anomalies are published to the Kafka `anomalies` topic, the **Kafka Consumer** application will read from this topic and send the anomaly data to RabbitMQ. The consumer connects to a RabbitMQ server, and pushes the data to a queue named `anomalies_queue`.

### Setting up RabbitMQ

Make sure RabbitMQ is running and that the `anomalies_queue` is created. You can check the data in RabbitMQ through the web interface (usually at `http://localhost:15672`).

### Using RabbitMQ with Node.js

Once the anomaly data is in RabbitMQ, it can be consumed by a Node.js application. Here's an example of how to connect to RabbitMQ and process the data:

1. **Install the `amqplib` Package**:
   ```bash
   npm install amqplib
   ```

2. **Consume Messages in Node.js**:
   Here is a simple example to consume data from RabbitMQ:
   ```javascript
   const amqp = require('amqplib/callback_api');

   amqp.connect('amqp://localhost', function (err, conn) {
     conn.createChannel(function (err, ch) {
       const queue = 'anomalies_queue';

       ch.assertQueue(queue, { durable: false });
       console.log('Waiting for messages in %s. To exit press CTRL+C', queue);

       ch.consume(queue, function (msg) {
         console.log('Received:', msg.content.toString());
         // Process data and post to Social Networks or PostgreSQL
       }, { noAck: true });
     });
   });
   ```

3. **Post to Social Networks or PostgreSQL**:
   You can use HTTP requests or a PostgreSQL client library like `pg` to post the results to Social Networks or PostgreSQL.

## Credits

- **Kafka**: [Apache Kafka](https://kafka.apache.org/)
- **Smile Library**: [Smile - Statistical Machine Intelligence and Learning Engine](https://haifengl.github.io/smile/)
- **RabbitMQ**: [RabbitMQ](https://www.rabbitmq.com/)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
