package gov.cdc.etldatapipeline.organization.service;


import gov.cdc.etldatapipeline.organization.model.dto.organization.OrgKey;
import gov.cdc.etldatapipeline.organization.model.dto.organization.OrgSp;
import gov.cdc.etldatapipeline.organization.model.odse.Organization;
import gov.cdc.etldatapipeline.organization.repository.OrganizationRepository;
import gov.cdc.etldatapipeline.organization.utils.StreamsSerdes;
import gov.cdc.etldatapipeline.organization.utils.UtilHelper;
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

import java.util.Set;
import java.util.stream.Collectors;


@Service
@RequiredArgsConstructor
@Setter
@Slf4j
public class KafkaStreamsService {
    @Value("#{kafkaConfig.getOrganizationTopic()}")
    private String organizationTopicName;
    @Value("#{kafkaConfig.getOrganizationElasticSearchTopic()}")
    private String organizationElasticSearchTopic;
    @Value("#{kafkaConfig.getOrganizationReportingTopic()}")
    private String organizationReportingOutputTopic;

    private final OrganizationRepository organizationRepository;

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {

        UtilHelper utilHelper = UtilHelper.getInstance();
        KStream<String, Set<OrgSp>> organizationKStream
                = streamsBuilder.stream(organizationTopicName, Consumed.with(Serdes.String(), Serdes.String()))
                .map((k, v) -> new KeyValue<>(
                        k,
                        UtilHelper.getInstance().deserializePayload(v, "/payload/after", Organization.class)))
                // KStream<String, Person>
                .filter((k, v) -> v != null)
                .peek((key, value) -> log.info("Received Person : " + value.getOrganizationUid()))
                .mapValues(v -> organizationRepository.computeAllOrganizations(v.getOrganizationUid()));


        // KStream<String, Set<Organization>>
        organizationKStream.flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(
                                utilHelper.constructDataEnvelope(new OrgKey(p.getOrganizationUid())),
                                utilHelper.constructDataEnvelope(p.processOrgElastic())))
                        .collect(Collectors.toSet()))
                .peek((key, value) -> log.info("Patient Elastic : {}", value.toString()))
                .to((key, v, recordContext) -> organizationElasticSearchTopic,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));

        // KStream<String, Set<Organization>>
        organizationKStream.flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(
                                utilHelper.constructDataEnvelope(new OrgKey(p.getOrganizationUid())),
                                utilHelper.constructDataEnvelope(p.processOrgReporting())))
                        .collect(Collectors.toSet()))
                .peek((key, value) -> log.info("Patient Reporting : {}", value.toString()))
                .to((key, v, recordContext) -> organizationReportingOutputTopic,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));

    }
}
