package com.datacenter.schemaregistry.serialization.kafka;



import com.datacenter.schemaregistry.serialization.AvroDataDeserializer;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by zuoc on 2017/5/26.
 */
public class KafkaAvroDeserializer implements Deserializer {

    private AvroDataDeserializer avroDataDeserializer;

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return avroDataDeserializer.deserialize(data);
    }

    @Override
    public void close() {
        avroDataDeserializer.close();
    }
}
