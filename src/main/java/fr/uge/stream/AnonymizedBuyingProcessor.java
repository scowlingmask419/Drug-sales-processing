package fr.uge.stream;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * Provides a producer/consumer using DSL API.
 */
public class AnonymizedBuyingProcessor {
    private final String inputTopic;
    private final String outputTopic;
    private final Injection<GenericRecord,byte[]> genericRecordInjection;
    private final static String ENCRYPTED_STRING = "***";
    private final static double MIN_PRICE = 4.0d;

    /**
     * Builds a processor.
     * @param pathSchema the avro schema path from content root
     * @param inputTopic the targeted data consumption topic
     * @param outputTopic the topic within which to produce the output data
     * @throws IOException
     * @throws SQLException
     */
    public AnonymizedBuyingProcessor(String pathSchema, String inputTopic, String outputTopic) throws IOException, SQLException {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        File file = new File(pathSchema);
        Schema schema = new Schema.Parser().parse(file);
        this.genericRecordInjection = GenericAvroCodecs.toBinary(schema);
    }

    /**
     * Processes the data from inputTopic topic and sends it to outputTopic topic.
     * @throws InterruptedException
     */
    public void run() throws InterruptedException {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "simpleKStreams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        StreamsConfig streamsConfig = new StreamsConfig(config);
        Serde<String> stringSerde = Serdes.String();
        Serde<byte[]> byteSerde = Serdes.ByteArray();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, byte[]> sourceStream = builder.stream(
                inputTopic,
                Consumed.with(stringSerde,
                byteSerde));

        KStream<String, String> anonymizedStream = sourceStream.mapValues(value -> {
                GenericRecord record = this.genericRecordInjection.invert(value).get();
                record.put("nom", ENCRYPTED_STRING);
                record.put("prenom", ENCRYPTED_STRING);
                return record;
            })
                // Filter records with price greater than MIN_PRICE
                .filter((key, value) -> {
                    String price = String.valueOf(value.get("prix"));
                    return Double.valueOf(price) > MIN_PRICE;
                })
                .mapValues(value -> value.toString());

        anonymizedStream.to(outputTopic, Produced.with(stringSerde, stringSerde));
        anonymizedStream.print(Printed.<String, String>toSysOut().withLabel("Anonymized drug sales with price greater than 4.0€"));
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        kafkaStreams.start();
        Thread.sleep(35000);
        kafkaStreams.close();
    }

    public static void main(String[] args) throws InterruptedException, IOException, SQLException {
        AnonymizedBuyingProcessor processor = new AnonymizedBuyingProcessor("src/main/resources/schema.avsc", "buying", "anonymizedBuying");
        processor.run();
    }


}
