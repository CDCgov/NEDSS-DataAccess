package gov.cdc.etldatapipeline.changedata.service;

import gov.cdc.etldatapipeline.changedata.model.dto.PersonOp;
import gov.cdc.etldatapipeline.changedata.model.odse.Person;
import gov.cdc.etldatapipeline.changedata.repository.PersonRepository;
import gov.cdc.etldatapipeline.changedata.utils.StreamsSerdes;
import gov.cdc.etldatapipeline.changedata.utils.UtilHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;


@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaStreamsService {
    private final PersonRepository personRepository;
    @Value("#{kafkaConfig.getPersonTopicName()}")
    private String personTopicName;
    @Value("#{kafkaConfig.getPatientAggregateTopicName()}")
    private String patientOutputTopicName;
    @Value("#{kafkaConfig.getProviderAggregateTopicName()}")
    private String providerOutputTopicName;

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {

        KStream<String, Person> personKStream
                = streamsBuilder.stream(personTopicName, Consumed.with(Serdes.String(), Serdes.String()))
                .map((k, v) -> new KeyValue<>(
                        k,
                        UtilHelper.getInstance().deserializePayload(v, "/payload/after", Person.class)))
                // KStream<String, Person>
                .filter((k, v) -> v != null)
                .peek((key, value) -> log.info("Received Person : " + value.getPersonUid()));

        KStream<String, PersonOp> consolidatedPatientStream = personKStream
                .mapValues(v -> personRepository.computeAllPatients(v.getPersonUid()))
                //KStream<String, List<Patient>>
                .flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(p.getPersonUid(), p.processPatient()))
                        .collect(Collectors.toSet()))
                // KStream<String, Patient>
                .peek((key, value) -> log.info("Patient Info : {}", value.toString()));

        consolidatedPatientStream.filter((k,v) -> v.getCd().equalsIgnoreCase("PAT"))
                .to(patientOutputTopicName, Produced.with(Serdes.String(), StreamsSerdes.PatientSerde()));
        consolidatedPatientStream.filter((k,v) -> v.getCd().equalsIgnoreCase("PRV"))
                .to(providerOutputTopicName, Produced.with(Serdes.String(), StreamsSerdes.PatientSerde()));

    }
}
