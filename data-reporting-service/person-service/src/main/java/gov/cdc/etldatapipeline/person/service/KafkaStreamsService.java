package gov.cdc.etldatapipeline.person.service;

import gov.cdc.etldatapipeline.person.model.dto.patient.PatientElasticSearch;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientKey;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientReporting;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientSp;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderElasticSearch;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderKey;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderReporting;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderSp;
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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
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
    @Value("#{kafkaConfig.getPatientElasticSearchTopic()}")
    private String patientElasticSearchTopicName;
    @Value("#{kafkaConfig.getPatientReportingTopic()}")
    private String patientReportingOutputTopic;
    @Value("#{kafkaConfig.getProviderElasticSearchTopic()}")
    private String providerElasticSearchOutputTopic;
    @Value("#{kafkaConfig.getProviderReportingTopic()}")
    private String providerReportingOutputTopic;

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {
        UtilHelper utilHelper = new UtilHelper();
        KStream<String, Person> personKStream
                = streamsBuilder.stream(personTopicName, Consumed.with(Serdes.String(), Serdes.String()))
                .map((k, v) -> new KeyValue<>(
                        k,
                        UtilHelper.getInstance().deserializePayload(v, "/payload/after", Person.class)))
                // KStream<String, Person>
                .filter((k, v) -> v != null)
                .peek((key, value) -> log.info("Received Person : " + value.getPersonUid()));

        KStream<String, List<PatientSp>> patientStream = personKStream
                .filter((k, v) -> v.getCd() != null && v.getCd().equalsIgnoreCase("PAT"))
                .mapValues(v -> patientRepository.computePatients(v.getPersonUid()));

        // KStream<String, List<Patient>>
        patientStream.flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(
                                utilHelper.constructDataEnvelope(PatientKey.build(p)),
                                utilHelper.constructDataEnvelope(PatientElasticSearch.build(p))))
                        .collect(Collectors.toSet()))
                .peek((key, value) -> log.info("Patient Elastic : {}", value.toString()))
                .to((key, v, recordContext) -> patientElasticSearchTopicName,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));
        // KStream<String, List<Patient>>
        patientStream.flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(
                                utilHelper.constructDataEnvelope(PatientKey.build(p)),
                                utilHelper.constructDataEnvelope(PatientReporting.build(p))))
                        .collect(Collectors.toSet()))
                .peek((key, value) -> log.info("Patient Reporting : {}", value.toString()))
                .to((key, v, recordContext) -> patientReportingOutputTopic,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));

        KStream<String, List<ProviderSp>> providerStream = personKStream
                .filter((k, v) -> v.getCd() != null && v.getCd().equalsIgnoreCase("PRV"))
                .mapValues(v -> providerRepository.computeProviders(v.getPersonUid()));

        // KStream<String, List<Provider>>
        providerStream
                .flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(
                                utilHelper.constructDataEnvelope(ProviderKey.build(p)),
                                utilHelper.constructDataEnvelope(ProviderReporting.build(p))))
                        .collect(Collectors.toSet()))
                .peek((key, value) -> log.info("Provider : {}", value.toString()))
                .to((key, v, recordContext) -> providerReportingOutputTopic,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));

        providerStream
                .flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(
                                utilHelper.constructDataEnvelope(ProviderKey.build(p)),
                                utilHelper.constructDataEnvelope(ProviderElasticSearch.build(p))))
                        .collect(Collectors.toSet()))
                .peek((key, value) -> log.info("Provider : {}", value.toString()))
                .to((key, v, recordContext) -> providerElasticSearchOutputTopic,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));
    }
}
