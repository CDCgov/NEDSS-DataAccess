package gov.cdc.etldatapipeline.changedata.utils;

import gov.cdc.etldatapipeline.changedata.model.dto.OrganizationOP;
import gov.cdc.etldatapipeline.changedata.model.dto.PatientFull;
import gov.cdc.etldatapipeline.changedata.model.dto.ProviderFull;
import gov.cdc.etldatapipeline.changedata.model.odse.Person;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StreamsSerdes extends Serdes {

    public static Serde<PatientFull> PatientSerde() {
        return new PatientSerde();
    }

    public static Serde<ProviderFull> ProviderSerde() {
        return new ProviderSerde();
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

    public static final class PatientSerde extends WrapperSerde<PatientFull> {
        public PatientSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(PatientFull.class, false));
        }
    }

    public static final class ProviderSerde extends WrapperSerde<ProviderFull> {
        public ProviderSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(ProviderFull.class, false));
        }
    }

    public static final class PersonSerde extends WrapperSerde<Person> {
        public PersonSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(Person.class, false));
        }
    }
}
