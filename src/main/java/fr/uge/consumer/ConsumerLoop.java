package fr.uge.consumer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Provides a consumer for sales per drugs analysis.
 */
public class ConsumerLoop implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ConsumerLoop.class.getName());
    private final KafkaConsumer<String, byte[]> consumer;
    private final List<String> topics;
    private final int id;
    private final Map<String, Double> drugSales = new HashMap<>();
    private final Injection<GenericRecord, byte[]> genericRecordInjection;

    private final Object lock = new Object();

    public ConsumerLoop(int id,
                        String groupId,
                        List<String> topics) throws IOException {
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);

        File file = new File("src/main/resources/schema.avsc");
        Schema schema = new Schema.Parser().parse(file);
        this.genericRecordInjection = GenericAvroCodecs.toBinary(schema);
    }

    /**
     * Gets the two most selled drugs according to their sellings sum.
     * @return a set of (cip, sellingSum) pairs
     */
    private Set<Map.Entry<String, Double>> getTopTwo() {
        return this.drugSales.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(2)
                .collect(Collectors.toSet());
    }

    /**
     * Processes the targeted topics.
     */
    @Override
    public void run() {
        try {
            consumer.subscribe(topics);

            while (true) {
                Duration timeout = Duration.ofSeconds(1);
                ConsumerRecords<String, byte[]> records = consumer.poll(timeout);
                for (ConsumerRecord<String, byte[]> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    GenericRecord genericRecord = this.genericRecordInjection.invert(record.value()).get();
                    String cip = String.valueOf(genericRecord.get("cip"));
                    String sellingPrice = String.valueOf(genericRecord.get("prix"));
                    synchronized (lock) {
                        drugSales.merge(cip, Double.valueOf(sellingPrice), Double::sum);
                        if (drugSales.size() >= 2) {
                            StringJoiner joiner = new StringJoiner(",","{", "}");
                            getTopTwo().forEach(entry -> {
                                joiner.add("cip=" + entry.getKey() + " : sellingSum=" + entry.getValue());
                            });
                            LOGGER.info("Top 2 : " + joiner);
                        }
                    }
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }
    public void shutdown() {
        consumer.wakeup();
    }
}

