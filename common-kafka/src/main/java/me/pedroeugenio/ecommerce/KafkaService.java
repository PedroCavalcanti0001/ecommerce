package me.pedroeugenio.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private KafkaConsumer<String, T> consumer;
    private ConsumerFunction parse;

    public KafkaService(String name, String topic, ConsumerFunction parse, Class<T> classType, Map<String, String> properties) {
        this(parse, name, classType, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(String name, Pattern topic, ConsumerFunction parse, Class<T> classType, Map<String, String> properties) {
        this(parse, name, classType, properties);
        consumer.subscribe(topic);
    }


    public KafkaService(ConsumerFunction parse, String name, Class<T> classType, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(classType, name, properties));
    }


    private Properties getProperties(Class<T> type, String name, Map<String, String> overrideProperties) {
        System.out.println("type " + type.getName());
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, name);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);

        for (Map.Entry<Object, Object> in : properties.entrySet()) {
            System.out.println("valor " + in.getKey() + " = " + in.getValue());
        }
        return properties;
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros!");
            } else continue;
            for (ConsumerRecord<String, T> record : records) {
                try {
                    parse.consume(record);
                } catch (Exception e) {
                    // only catches Exception because no matter which Exception
                    //i want to recover and parse the next one
                    // so far, just logging the exception for this message
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
