package gov.cdc.etldatapipeline.changedata.service;

import gov.cdc.etldatapipeline.changedata.model.dto.PatientOP;
import gov.cdc.etldatapipeline.changedata.model.odse.Person;
import gov.cdc.etldatapipeline.changedata.repository.PatientRepository;
import gov.cdc.etldatapipeline.changedata.utils.StreamsSerdes;
import gov.cdc.etldatapipeline.changedata.utils.UtilHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class PatientConsumerService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private final UtilHelper utilHelper = UtilHelper.getInstance();

    private final String personTopicName;
    private final String patientOutputTopicName;
    private final PatientRepository patientRepository;

    public void processPatientData(StreamsBuilder streamsBuilder) {
        KStream<String, Person> personKStream
                = streamsBuilder.stream(personTopicName, Consumed.with(STRING_SERDE, STRING_SERDE))
                .map((k, v) -> new KeyValue<>(
                        k,
                        utilHelper.deserializePayload(v, "/payload/after", Person.class)))
                // KStream<String, Person>
                .peek((key, value) -> log.info("Calling the Patient Repository for " + value.getPersonUid()));

        KStream<String, PatientOP> consolidatedPatientStream = personKStream
                .mapValues(v -> patientRepository.computeAllPatients(v.getPersonUid()))
                //KStream<String, List<Patient>>
                .flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(p.getPatientUid(), p))
                        .collect(Collectors.toSet()))
                // KStream<String, Patient>
                .peek((key, value) -> log.info("Patient Info : {}", value.toString()));

        consolidatedPatientStream.to(patientOutputTopicName,
                Produced.with(Serdes.String(), StreamsSerdes.PatientSerde()));
    }

}


