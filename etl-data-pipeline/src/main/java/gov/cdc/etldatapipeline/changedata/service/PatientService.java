package gov.cdc.etldatapipeline.changedata.service;

import gov.cdc.etldatapipeline.changedata.model.odse.CtContact;
import gov.cdc.etldatapipeline.changedata.model.odse.Participation;
import gov.cdc.etldatapipeline.changedata.model.odse.Person;
import gov.cdc.etldatapipeline.changedata.model.dto.InitPatient;
import gov.cdc.etldatapipeline.changedata.repository.PatientRepository;
import gov.cdc.etldatapipeline.changedata.utils.StreamsSerdes;
import gov.cdc.etldatapipeline.changedata.utils.UtilHelper;
import io.debezium.serde.json.JsonSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

@RequiredArgsConstructor
@Slf4j
public class PatientService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private static final Serde<Person> PERSON_SERDE =
            StreamsSerdes.PersonSerde();
    private static final Serde<Participation> PARTICIPATION_SERDE =
            StreamsSerdes.ParticipationSerde();
    private static final Serde<CtContact> CT_CONTACT_SERDE =
            StreamsSerdes.CtContactSerde();

    private static final String PERSON_STORE = "person-store";
    private static final String PARTICIPATION_STORE = "participation-store";
    private static final String CT_CONTACT_STORE = "ct-contact-store";
    private static final String INIT_PATIENT_STORE = "init-patient-store";

    private final UtilHelper utilHelper = UtilHelper.getInstance();

    private final String personTopicName;
    private final String participationTopicName;
    private final String ctContactTopicName;
    private final PatientRepository patientRepository;


    public void processPatientData(StreamsBuilder streamsBuilder) {
        KStream<String, Person> personInputKStream =
                streamsBuilder.stream(
                                personTopicName,
                                Consumed.with(STRING_SERDE, STRING_SERDE))
                        .map((k, v) -> new KeyValue<>(
                                utilHelper.parseJsonNode(k,
                                        "/payload/person_uid", String.class),
                                utilHelper.deserializePayload(v,
                                        "/payload/after", Person.class)));
        KTable<String, Person> personTable
                = personInputKStream.toTable(
                Materialized.<String, Person, KeyValueStore<Bytes, byte[]>>as(
                                PERSON_STORE)
                        .withKeySerde(STRING_SERDE)
                        .withValueSerde(PERSON_SERDE)
                        .withRetention(Duration.ofDays(1)));
        personTable.toStream().foreach((k, v) -> log.info(
                "personTable :: Key :: {}, Value :: {}", k, v));

        KStream<String, Participation> participationInputKStream =
                streamsBuilder.stream(
                                participationTopicName,
                                Consumed.with(STRING_SERDE, STRING_SERDE))
                        .map((k, v) -> new KeyValue<>(
                                utilHelper.parseJsonNode(k,
                                        "/payload/subject_entity_uid",
                                        String.class),
                                utilHelper.deserializePayload(v,
                                        "/payload/after",
                                        Participation.class)));

        KTable<String, Participation> participationTable
                = participationInputKStream.toTable(
                Materialized.<String, Participation, KeyValueStore<Bytes,
                                byte[]>>as(
                                PARTICIPATION_STORE)
                        .withKeySerde(STRING_SERDE)
                        .withValueSerde(PARTICIPATION_SERDE)
                        .withRetention(Duration.ofDays(1)));

        participationTable.toStream().foreach((k, v) -> log.info(
                "participationTable :: Key :: {}, Value :: {}", k, v));
        KStream<String, CtContact> ctContactInputKStream =
                streamsBuilder.stream(
                                ctContactTopicName,
                                Consumed.with(STRING_SERDE, STRING_SERDE))
                        .map((k, v) -> new KeyValue<>(
                                utilHelper.parseJsonNode(k,
                                        "/payload/subject_entity_uid",
                                        String.class),
                                utilHelper.deserializePayload(v,
                                        "/payload/after", CtContact.class)));

        KTable<String, CtContact> ctContactTable
                = ctContactInputKStream.toTable(
                Materialized.<String, CtContact,
                                KeyValueStore<Bytes, byte[]>>as(
                                CT_CONTACT_STORE)
                        .withKeySerde(STRING_SERDE)
                        .withValueSerde(CT_CONTACT_SERDE)
                        .withRetention(Duration.ofDays(1)));

        ctContactTable.toStream().foreach((k, v) -> log.info(
                "ctContactTable :: Key :: {}, Value :: {}", k, v));

        KTable<String, InitPatient> patientPartTable = personTable.join(
                participationTable,
                (person, participation) ->
                        new InitPatient().constructPatient(person,
                                participation));
        patientPartTable.toStream().foreach((k, v) -> log.info(
                "patientPartTable :: Key :: {}, Value :: {}", k, v));
        KTable<String, InitPatient> patientCtContactTable = personTable.join(
                ctContactTable,
                (person, ctContact) ->
                        new InitPatient().constructPatient(person,
                                ctContact));
        patientCtContactTable.toStream().foreach((k, v) -> log.info(
                "patientCtContactTable :: Key :: {}, Value :: {}", k, v));
        KTable<String, InitPatient> initPatientKTable =
                patientPartTable.toStream().merge(patientCtContactTable.toStream()).toTable(
                        Materialized.<String, InitPatient,
                                        KeyValueStore<Bytes, byte[]>>as(
                                        INIT_PATIENT_STORE)
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(new JsonSerde<>(InitPatient.class))
                                .withRetention(Duration.ofDays(1)));
        initPatientKTable.toStream().foreach((k, v) -> log.info(
                "initPatientKTable :: Key :: {}, Value :: {}", k, v));
        initPatientKTable.toStream().foreach((k,v) -> patientRepository.save(v));
    }
}
