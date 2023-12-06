package gov.cdc.etldatapipeline.changedata.utils;

import gov.cdc.etldatapipeline.changedata.model.odse.NbsPage;
import gov.cdc.etldatapipeline.changedata.model.odse.CtContact;
import gov.cdc.etldatapipeline.changedata.model.odse.Participation;
import gov.cdc.etldatapipeline.changedata.model.odse.Person;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StreamsSerdes extends Serdes {

    public static Serde<NbsPage> NbsPageSerde() {
        return new NbsPageSerde();
    }

    public static Serde<Person> PersonSerde() {
        return new PersonSerde();
    }

    public static Serde<Participation> ParticipationSerde() {
        return new ParticipationSerde();
    }

    public static Serde<CtContact> CtContactSerde() {
        return new CtContactSerde();
    }

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

    public static final class ParticipationSerde extends WrapperSerde<Participation> {
        public ParticipationSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(Participation.class, false));
        }
    }

    public static final class CtContactSerde extends WrapperSerde<CtContact> {
        public CtContactSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(CtContact.class, false));
        }
    }
}
