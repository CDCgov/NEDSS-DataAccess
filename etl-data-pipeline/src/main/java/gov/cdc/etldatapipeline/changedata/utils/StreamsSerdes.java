package gov.cdc.etldatapipeline.changedata.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import gov.cdc.etldatapipeline.changedata.model.odse.*;
import gov.cdc.etldatapipeline.changedata.model.dto.Provider;
import gov.cdc.etldatapipeline.changedata.model.dto.OrganizationOP;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.List;

public class StreamsSerdes extends Serdes {

    public static Serde<NbsPage> NbsPageSerde() {
        return new NbsPageSerde();
    }

    public static Serde<Person> PersonSerde() {
        return new PersonSerde();
    }

    public static Serde<Provider> ProviderSerde() {
        return new ProviderSerde();
    }

    public static Serde<List<String>> StringListSerde() {
        return new StringListSerde();
    }

    public static Serde<Participation> ParticipationSerde() {
        return new ParticipationSerde();
    }

    public static Serde<CtContact> CtContactSerde() {
        return new CtContactSerde();
    }

    public static Serde<OrganizationOP> OrganizationSerde() {return new OrganizationSerde();}

    public static final class NbsPageSerde extends WrapperSerde<NbsPage> {
        public NbsPageSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(NbsPage.class, false));
        }
    }

    public static final class PersonSerde extends WrapperSerde<Person> {
        public PersonSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Person.class
                    , false));
        }
    }

    public static final class ProviderSerde extends WrapperSerde<Provider> {
        public ProviderSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(Provider.class, false));
        }
    }

    public static final class StringListSerde extends WrapperSerde<List<String>> {
        public StringListSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(new TypeReference<>() {
            }, false));
        }
    }

    public static final class ParticipationSerde extends WrapperSerde<Participation> {
        public ParticipationSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(Participation.class, false));
        }
    }

    public static final class CtContactSerde extends WrapperSerde<CtContact> {
        public CtContactSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(CtContact.class, false));
        }
    }

    public static final class OrganizationSerde extends WrapperSerde<OrganizationOP> {
        public OrganizationSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(OrganizationOP.class, false));
        }
    }
}
