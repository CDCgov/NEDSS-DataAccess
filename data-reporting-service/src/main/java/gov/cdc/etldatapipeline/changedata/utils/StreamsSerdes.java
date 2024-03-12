package gov.cdc.etldatapipeline.changedata.utils;

import gov.cdc.etldatapipeline.changedata.model.dto.OrganizationOP;
import gov.cdc.etldatapipeline.changedata.model.dto.PersonFull;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StreamsSerdes extends Serdes {

    public static Serde<PersonFull> PatientSerde() {
        return new PatientSerde();
    }

    public static Serde<OrganizationOP> OrganizationSerde() {return new OrganizationSerde();}

    public static final class OrganizationSerde extends WrapperSerde<OrganizationOP> {
        public OrganizationSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(OrganizationOP.class, false));
        }
    }

    public static final class PatientSerde extends WrapperSerde<PersonFull> {
        public PatientSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(PersonFull.class, false));
        }
    }
}
