package gov.cdc.etldatapipeline.person.utils;

import gov.cdc.etldatapipeline.person.model.dto.DataEnvelope;
import gov.cdc.etldatapipeline.person.model.dto.OrganizationOP;
import gov.cdc.etldatapipeline.person.model.odse.Person;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StreamsSerdes extends Serdes {

    public static Serde<DataEnvelope> DataEnvelopeSerde() {
        return new DataEnvelopeSerde();
    }

    public static Serde<Person> PersonSerde() {
        return new PersonSerde();
    }

    public static Serde<OrganizationOP> OrganizationSerde() {return new OrganizationSerde();}

    public static final class OrganizationSerde extends WrapperSerde<OrganizationOP> {
        public OrganizationSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(OrganizationOP.class, false));
        }
    }

    public static final class DataEnvelopeSerde extends WrapperSerde<DataEnvelope> {
        public DataEnvelopeSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(DataEnvelope.class, false));
        }
    }

    public static final class PersonSerde extends WrapperSerde<Person> {
        public PersonSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(Person.class, false));
        }
    }
}
