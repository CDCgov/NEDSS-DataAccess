package gov.cdc.etldatapipeline.organization.service;

import gov.cdc.etldatapipeline.organization.model.dto.org.OrgElasticSearch;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrgKey;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrgReporting;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrgSp;
import gov.cdc.etldatapipeline.organization.model.odse.Organization;
import gov.cdc.etldatapipeline.organization.repository.OrgRepository;
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
    private String orgTopicName;
    @Value("#{kafkaConfig.getOrganizationElasticSearchTopic()}")
    private String orgElasticSearchTopic;
    @Value("#{kafkaConfig.getOrganizationReportingTopic()}")
    private String orgReportingOutputTopic;

    private final OrgRepository orgRepository;

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {

        UtilHelper utilHelper = UtilHelper.getInstance();
        KStream<String, Set<OrgSp>> organizationKStream
                = streamsBuilder.stream(orgTopicName, Consumed.with(Serdes.String(), Serdes.String()))
                .map((k, v) -> new KeyValue<>(
                        k,
                        UtilHelper.getInstance().deserializePayload(v, "/payload/after", Organization.class)))
                // KStream<String, Organization>
                .filter((k, v) -> v != null)
                .peek((key, value) -> log.info("Received Organization : " + value.getOrganizationUid()))
                .mapValues(v -> orgRepository.computeAllOrganizations(v.getOrganizationUid()));

        organizationKStream.flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(
                                utilHelper.buildAvroRecord(OrgKey.build(p)),
                                utilHelper.buildAvroRecord(OrgReporting.build(p))))
                        .collect(Collectors.toSet()))
                .peek((key, value) ->
                        log.info("Patient Reporting : {}", value.toString()))
                .to((key, v, recordContext) -> orgReportingOutputTopic,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));

        organizationKStream.flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(
                                utilHelper.buildAvroRecord(OrgKey.build(p)),
                                utilHelper.buildAvroRecord(OrgElasticSearch.build(p))))
                        .collect(Collectors.toSet()))
                .peek((key, value) ->
                        log.info("Patient Elastic : {}", value.toString()))
                .to((key, v, recordContext) -> orgElasticSearchTopic,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));

    }
}
