package gov.cdc.etldatapipeline.person.utils;

import gov.cdc.etldatapipeline.person.model.dto.OrganizationOP;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientEnvelope;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderEnvelope;
import gov.cdc.etldatapipeline.person.model.odse.Person;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StreamsSerdes extends Serdes {

    public static Serde<PatientEnvelope> PatientEnvelopeSerde() {
        return new PatientEnvelopeSerde();
    }

    public static Serde<ProviderEnvelope> ProviderEnvelopeSerde() {
        return new ProviderEnvelopeSerde();
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

    public static final class PatientEnvelopeSerde extends WrapperSerde<PatientEnvelope> {
        public PatientEnvelopeSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(PatientEnvelope.class, false));
        }
    }

    public static final class ProviderEnvelopeSerde extends WrapperSerde<ProviderEnvelope> {
        public ProviderEnvelopeSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(ProviderEnvelope.class, false));
        }
    }

    public static final class PersonSerde extends WrapperSerde<Person> {
        public PersonSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(Person.class, false));
        }
    }
}
