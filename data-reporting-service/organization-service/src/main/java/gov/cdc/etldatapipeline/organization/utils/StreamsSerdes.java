package gov.cdc.etldatapipeline.organization.utils;


import gov.cdc.etldatapipeline.organization.model.avro.DataEnvelope;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StreamsSerdes extends Serdes {

    public static Serde<DataEnvelope> DataEnvelopeSerde() {
        return new DataEnvelopeSerde();
    }

    public static final class DataEnvelopeSerde extends WrapperSerde<DataEnvelope> {
        public DataEnvelopeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(DataEnvelope.class, false));
        }
    }
}
