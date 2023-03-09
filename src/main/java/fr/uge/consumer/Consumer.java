package fr.uge.consumer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Provides a consumer and a producer for drug buying records.
 */
public class Consumer {
    private static final Logger LOGGER = Logger.getLogger(Consumer.class.getName());
    private final KafkaConsumer<String, byte[]> consumer;
    private final KafkaProducer<String, byte[]> producer;
    private final Injection<GenericRecord, byte[]> genericRecordInjection;
    private final String producerTopic;

    /**
     * Builds a consumer instance.
     * @param pathSchema the avro schema path from content root
     * @param consumerTopic the targeted data consumption topic
     * @param producerTopic the topic within which to produce the output data
     * @throws IOException
     */
    public Consumer(String pathSchema, String consumerTopic, String producerTopic) throws IOException {
        Properties propertiesConsumer = new Properties();
        propertiesConsumer.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        propertiesConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propertiesConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        propertiesConsumer.put("group.id", "group1");

        Properties propertiesProducer = new Properties();
        propertiesProducer.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        propertiesProducer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propertiesProducer.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.consumer = new KafkaConsumer<String, byte[]>(propertiesConsumer);
        this.producer = new KafkaProducer<String, byte[]>(propertiesProducer);

        File file = new File(pathSchema);
        Schema schema = new Schema.Parser().parse(file);
        this.genericRecordInjection = GenericAvroCodecs.toBinary(schema);

        this.consumer.subscribe(Arrays.asList(consumerTopic));

        this.producerTopic = producerTopic;
    }

    /**
     * Fetches data from a topic and sends the data with a new key (i.e., cip from drugs4projet).
     */
    public void fetchFromBuyingLoop() {
        boolean running = true;
        Duration oneSecond = Duration.ofSeconds(1);
        while (running) {
            ConsumerRecords<String, byte[]> records = this.consumer.poll(oneSecond);
            for (ConsumerRecord<String, byte[]> record : records) {
                GenericRecord genericRecord = this.genericRecordInjection.invert(record.value()).get();
                LOGGER.info("Fetched record on partition=" + record.partition() + ", value=" + genericRecord);
                String cip = String.valueOf(genericRecord.get("cip"));
                ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(this.producerTopic, cip, record.value());
                this.producer.send(producerRecord);
                LOGGER.info("Sent record : " + producerRecord);
            }
        }
        this.consumer.close();
    }

    public static void main(String[] args) throws IOException {
        Consumer buying = new Consumer("src/main/resources/schema.avsc", "buying", "top2");
        buying.fetchFromBuyingLoop();
    }
}
