package org.aghersi;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;
import com.google.gson.Gson;

public class AccumulatorSerde extends Serdes.WrapperSerde<Accumulator> {
    public AccumulatorSerde() {
        super(new AccumulatorSerializer(), new AccumulatorDeserializer());
    }

    public static class AccumulatorSerializer implements Serializer<Accumulator> {
        private final Gson gson = new Gson();

        @Override
        public byte[] serialize(String topic, Accumulator data) {
            return gson.toJson(data).getBytes();
        }
    }

    public static class AccumulatorDeserializer implements Deserializer<Accumulator> {
        private final Gson gson = new Gson();

        @Override
        public Accumulator deserialize(String topic, byte[] data) {
            return gson.fromJson(new String(data), Accumulator.class);
        }
    }
}
