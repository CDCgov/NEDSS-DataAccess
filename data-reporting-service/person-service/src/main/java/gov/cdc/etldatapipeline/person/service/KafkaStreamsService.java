package gov.cdc.etldatapipeline.person.service;

import gov.cdc.etldatapipeline.person.model.odse.Person;
import gov.cdc.etldatapipeline.person.repository.PatientRepository;
import gov.cdc.etldatapipeline.person.repository.ProviderRepository;
import gov.cdc.etldatapipeline.person.utils.StreamsSerdes;
import gov.cdc.etldatapipeline.person.utils.UtilHelper;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;


@Service
@RequiredArgsConstructor
@Setter
@Slf4j
public class KafkaStreamsService {
    private final PatientRepository patientRepository;
    private final ProviderRepository providerRepository;
    @Value("#{kafkaConfig.getPersonTopicName()}")
    private String personTopicName;
    @Value("#{kafkaConfig.getPatientAggregateTopicName()}")
    private String patientOutputTopicName;
    @Value("#{kafkaConfig.getProviderAggregateTopicName()}")
    private String providerOutputTopicName;
    @Value("#{kafkaConfig.getDefaultDataTopicName()}")
    private String defaultDataTopicName;

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

        personKStream.split()
                .branch((k, v) -> v.getCd() != null && v.getCd().equalsIgnoreCase("PAT"),
                        Branched.withConsumer(ks -> ks
                                .mapValues(v ->
                                        patientRepository.computePatients(v.getPersonUid()))
                                //KStream<String, List<Patient>>
                                .flatMap((k, v) -> v.stream()
                                        .map(p -> KeyValue.pair(p.getPersonUid(), p.constructPatientEnvelope()))
                                        .collect(Collectors.toSet()))
                                .peek((key, value) -> log.info("Patient : {}", value.toString()))
                                .to((key, v, recordContext) -> patientOutputTopicName,
                                        Produced.with(Serdes.Long(), StreamsSerdes.PatientEnvelopeSerde()))))
                .branch((k, v) -> v.getCd() != null && v.getCd().equalsIgnoreCase("PRV"),
                        Branched.withConsumer(ks -> ks
                                .mapValues(v ->
                                        providerRepository.computeProviders(v.getPersonUid()))
                                //KStream<String, List<Patient>>
                                .flatMap((k, v) -> v.stream()
                                        .map(p -> KeyValue.pair(p.getPersonUid(), p.constructPatientEnvelope()))
                                        .collect(Collectors.toSet()))
                                .peek((key, value) -> log.info("Provider : {}", value.toString()))
                                .to((key, v, recordContext) -> providerOutputTopicName,
                                        Produced.with(Serdes.Long(), StreamsSerdes.ProviderEnvelopeSerde()))))
                .defaultBranch(Branched.withConsumer(ks -> ks.to(defaultDataTopicName,
                        Produced.with(Serdes.String(), StreamsSerdes.PersonSerde()))));
    }
}
