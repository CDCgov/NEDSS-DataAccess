package gov.cdc.etldatapipeline.organization.service;

import gov.cdc.etldatapipeline.commonutil.json.StreamsSerdes;
import gov.cdc.etldatapipeline.organization.model.dto.org.OrganizationSp;
import gov.cdc.etldatapipeline.organization.model.odse.Organization;
import gov.cdc.etldatapipeline.organization.repository.OrgRepository;
import gov.cdc.etldatapipeline.organization.transformer.OrganizationTransformers;
import gov.cdc.etldatapipeline.organization.transformer.OrganizationType;
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
public class OrganizationService {
    @Value("#{kafkaConfig.getOrganizationTopic()}")
    private String orgTopicName;
    @Value("#{kafkaConfig.getOrganizationElasticSearchTopic()}")
    private String orgElasticSearchTopic;
    @Value("#{kafkaConfig.getOrganizationReportingTopic()}")
    private String orgReportingOutputTopic;

    private final OrgRepository orgRepository;
    private final OrganizationTransformers transformer;

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {
        KStream<String, Set<OrganizationSp>> organizationKStream
                = streamsBuilder.stream(orgTopicName, Consumed.with(Serdes.String(), Serdes.String()))
                .map((key, value) -> new KeyValue<>(
                        key,
                        UtilHelper.getInstance().deserializePayload(value, "/payload/after", Organization.class)))
                // KStream<String, Organization>
                .filter((key, value) -> value != null)
                .peek((key, value) -> log.info("Received Organization : " + value.getOrganizationUid()))
                .mapValues(value -> orgRepository.computeAllOrganizations(value.getOrganizationUid()));

        organizationKStream.flatMap((key, value) -> value.stream()
                        .map(p -> KeyValue.pair(
                                transformer.buildOrganizationKey(p),
                                transformer.processData(p, OrganizationType.ORGANIZATION_REPORTING)))
                        .collect(Collectors.toSet()))
                .peek((key, value) ->
                        log.info("Organization Reporting : {}", value.toString()))
                .to((key, value, recordContext) -> orgReportingOutputTopic,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));

        organizationKStream.flatMap((key, value) -> value.stream()
                        .map(p -> KeyValue.pair(transformer.buildOrganizationKey(p),
                                transformer.processData(p, OrganizationType.ORGANIZATION_ELASTIC_SEARCH)))
                        .collect(Collectors.toSet()))
                .peek((key, value) ->
                        log.info("Organization Elastic : {}", value.toString()))
                .to((key, value, recordContext) -> orgElasticSearchTopic,
                        Produced.with(
                                StreamsSerdes.DataEnvelopeSerde(),
                                StreamsSerdes.DataEnvelopeSerde()));

    }
}