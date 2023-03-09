package fr.uge.producer;

import com.github.javafaker.Faker;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Provides a producer for buying records.
 */
public class Producer {
    private static final Logger LOGGER = Logger.getLogger(Producer.class.getName());
    private static final String URL = "jdbc:postgresql://localhost/postgres";
    private static final short PERCENTAGE = 10;

    private final Connection connection;

    /**
     * Builds a producer connected to a local PostgreSQL database.
     * @throws SQLException
     */
    public Producer() throws SQLException {
        //this.connection = DriverManager.getConnection(URL,"amyr.jaafri","BDD2022");
        this.connection = DriverManager.getConnection(URL,"postgres", "");
    }

    /**
     * Retrieves a full row from table drugs4projet.
     * @return a random row from table drugs4projet
     * @throws SQLException
     */
    private ResultSet getRandomRowDrug() throws SQLException {
        Statement statement = connection.createStatement();
        String sqlQuery = "SELECT * FROM drugs4projet ORDER BY RANDOM () LIMIT 1;";
        return statement.executeQuery(sqlQuery);
    }

    /**
     * Retrieves a full row from table pharm4projet.
     * @return a random row from table pharm4projet
     * @throws SQLException
     */
    private long getRandomIdPharma() throws SQLException {
        Statement statement = connection.createStatement();
        String sqlQuery = "SELECT * FROM pharm4projet ORDER BY RANDOM () LIMIT 1;";
        ResultSet resultSet = statement.executeQuery(sqlQuery);
        resultSet.next();
        return resultSet.getLong(1);
    }

    /**
     * Applies a random rise/cut to a price.
     * @param initPrice the initial price
     * @return the price raised or cutted from PERCENTAGE %
     */
    private double generatePrice(double initPrice) {
        boolean isRaised = new Random().nextBoolean();
        if (isRaised) {
            return initPrice * (1 + PERCENTAGE/100);
        }
        return initPrice * (1 - PERCENTAGE/100);
    }

    /**
     * Produces a record.
     * @return a record
     * @throws SQLException
     */
    public List<String> generateBuying() throws SQLException {
        Faker client = new Faker();
        ResultSet drugRow = getRandomRowDrug();
        drugRow.next();
        String cip = drugRow.getString(1);
        String price = String.valueOf(generatePrice(drugRow.getLong(2)));
        String idPharma = String.valueOf(getRandomIdPharma());
        return List.of(client.name().firstName(), client.name().lastName(), cip, price, idPharma);
    }

    public static void main(String[] args) throws SQLException, IOException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);

        File file = new File("src/main/resources/schema.avsc");
        Schema schema = new Schema.Parser().parse(file);
        Injection<GenericRecord, byte[]> injection = GenericAvroCodecs.toBinary(schema);
        GenericData.Record record = new GenericData.Record(schema);
        while (true) {
            List<String> infos = new Producer().generateBuying();
            record.put("nom", infos.get(0));
            record.put("prenom", infos.get(1));
            record.put("cip", infos.get(2));
            record.put("prix", infos.get(3));
            record.put("idPharma", infos.get(4));
            byte[] recordBytes = injection.apply(record);
            ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>("buying", recordBytes);
            producer.send(producerRecord);
            LOGGER.info("Sent record : " + record);
            try {
                Thread.sleep(250);
            } catch(InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
