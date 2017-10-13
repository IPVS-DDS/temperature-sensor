package de.unistuttgart.ipvs.dds;

import java.util.*;

import de.unistuttgart.ipvs.dds.avro.TemperatureData;
import de.unistuttgart.ipvs.dds.avro.TemperatureUnit;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TemperatureSensor {
    private final static Logger logger = LogManager.getLogger(TemperatureSensor.class);

    public static void main(String[] args) {
        /* Zookeeper server URLs */
        final String kafkaBroker = args.length > 0 ? args[0] : "localhost:9092";
        /* URL of the Schema Registry */
        final String schemaRegistry = args.length > 1 ? args[1] : "http://localhost:8081";

        /* SensorID to report */
        final String sensorId = args.length > 2 ? args[2] : UUID.randomUUID().toString();

        /* Temperature at which to start */
        final double baseTemperature = args.length > 3 ? Double.parseDouble(args[3]) : 0.0;

        /* Target temperature to reach */
        final double targetTemperature = args.length > 4 ? Double.parseDouble(args[4]) : 100.0;

        /* Temperature unit to report */
        final TemperatureUnit temperatureUnit = args.length > 5
                ? TemperatureUnit.valueOf(args[5])
                : TemperatureUnit.C;

        /* Client ID with which to register with Kafka */
        final String clientId = "temperature-sensor";

        /* The Kafka topic to which to send messages */
        final String outputTopic = "temperature-data";

        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        final SpecificAvroSerde<TemperatureData> helloSerde = createSerde(schemaRegistry);

        final long startTimestamp = System.currentTimeMillis();
        final Random random = new Random();

        try (KafkaProducer<String, TemperatureData> producer = new KafkaProducer<>(
                producerProperties,
                Serdes.String().serializer(),
                helloSerde.serializer()
        )) {
            long messageNumber = 0;
            while (true) {
                /*
                 * Generate some sample temperature data based on the runtime of the sensor. We'll simulate an
                 * increasing temperature using an inverse exponential function.
                 *
                 * TODO: Add generation of random temperature spikes that go away after some time
                 */
                final long now = System.currentTimeMillis();
                final double exponent = (startTimestamp - now) / 30000d;
                final double multiplier = targetTemperature - baseTemperature;
                final double temperatureBase = baseTemperature + (1 - Math.pow(Math.E, exponent)) * multiplier;
                final double temperatureNoise = random.nextDouble() * 0.1;
                final double temperature = temperatureBase + temperatureNoise;

                final TemperatureData message = new TemperatureData(sensorId, now, temperature, temperatureUnit);

                logger.info("Sending #" + ++messageNumber + " (" + (double)messageNumber / (now - startTimestamp) * 1000d + "/s): " + message.toString());
                producer.send(new ProducerRecord<>(outputTopic, sensorId, message));

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.info("Terminating because the thread was interrupted");
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Terminating because an error was encountered", e);
        }
    }

    /**
     * Creates a serialiser/deserialiser for the given type, registering the Avro schema with the schema registry.
     *
     * @param schemaRegistryUrl the schema registry to register the schema with
     * @param <T>               the type for which to create the serialiser/deserialiser
     * @return                  the matching serialiser/deserialiser
     */
    private static <T extends SpecificRecord> SpecificAvroSerde<T> createSerde(final String schemaRegistryUrl) {
        final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl
        );
        serde.configure(serdeConfig, false);
        return serde;
    }
}
