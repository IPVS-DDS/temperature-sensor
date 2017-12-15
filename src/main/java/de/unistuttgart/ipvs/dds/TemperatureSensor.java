package de.unistuttgart.ipvs.dds;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

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

        /* Amount of nose to add to the base signal */
        final double noiseAmount = args.length > 6 ? Double.parseDouble(args[6]) : 0.1;

        /* How long to wait between temperature measurements (in ms) */
        final long measurementInterval = (long) ((args.length > 7 ? Double.parseDouble(args[7]) : 1000) * 1000);

        /* Client ID with which to register with Kafka */
        final String clientId = "temperature-sensor";

        /* The Kafka topic to which to send messages */
        final String outputTopic = "temperature-data";

        final Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        final SpecificAvroSerde<TemperatureData> temperatureDataSerde = createSerde(schemaRegistry);

        final long startTimestamp = System.currentTimeMillis();
        final Random random = new Random();

        try (KafkaProducer<String, TemperatureData> producer = new KafkaProducer<>(
                producerProperties,
                Serdes.String().serializer(),
                temperatureDataSerde.serializer()
        )) {
            long messageNumber = 0;

            // Leave at least 20s of warmup before the first spike is triggered.
            long spikeStart = ThreadLocalRandom.current().nextLong(20000, 60000);
            // The first spike will always last about 5s.
            long spikeDuration = 5;
            long spikeEnd = 1000;
            while (true) {
                /*
                 * Generate some sample temperature data based on the runtime of the sensor. We'll simulate an
                 * increasing temperature using an inverse exponential function.
                 *
                 * TODO: Add generation of random temperature spikes that go away after some time
                 */
                final long now = System.currentTimeMillis();

                final long deltaT = now - startTimestamp;
                double temperatureBase = simulateTemperature(baseTemperature, targetTemperature, deltaT);
                if (now > spikeStart) {
                    if (now > spikeEnd) {
                        // The spike ended, set the parameters for the next one

                        // Create the next spike somewhere between now and 60 seconds in the future.
                        spikeStart = ThreadLocalRandom.current().nextLong(60000);
                        // Make the spike last between one and ten seconds.
                        spikeDuration = ThreadLocalRandom.current().nextLong(1000, 10000);
                        // The actual end of the whole spike is double its duration. During the spikeDuration, the
                        // temperature rises, but it takes the same time to drop down to the 'normal' level again.
                        spikeEnd = spikeStart + spikeDuration * 2;
                    } else if (now - spikeStart < spikeDuration) {
                        // Temperature spike in progress, temperature is rising
                        temperatureBase += simulateTemperature(0, 10, now - spikeStart);
                    } else {
                        // Temperature spike is over, cooling off again
                        temperatureBase += simulateTemperature(10, 0, now - (spikeStart + spikeDuration));
                    }
                }
                final double temperature = temperatureBase + simulateNoise(noiseAmount);

                final TemperatureData message = new TemperatureData(sensorId, now, temperature, temperatureUnit);

                logger.info("Sending #" + ++messageNumber + " (" + (double)messageNumber / (now - startTimestamp) * 1000d + "/s): " + message.toString());
                producer.send(new ProducerRecord<>(outputTopic, sensorId, message));

                if (measurementInterval < 1000) {
                    // We're in the nanosecond range, Thread.sleep() won't do.
                    final long endWait = System.nanoTime() + measurementInterval;
                    // A busy loop has to do.
                    while (System.nanoTime() < endWait) { }
                } else {
                    try {
                        Thread.sleep(measurementInterval / 1000);
                    } catch (InterruptedException e) {
                        logger.info("Terminating because the thread was interrupted");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Terminating because an error was encountered", e);
        }
    }

    private static double simulateTemperature(double baseTemp, double targetTemp, long deltaT) {
        return baseTemp + (1 - Math.pow(Math.E, -deltaT / 30000d)) * (targetTemp - baseTemp);
    }

    private static double simulateNoise(double amount) {
        return ThreadLocalRandom.current().nextDouble(-amount, amount);
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
