package gov.cdc.etldatapipeline.changedata.service;

import gov.cdc.etldatapipeline.changedata.model.dto.OrganizationOP;
import gov.cdc.etldatapipeline.changedata.repository.OrganizationRepository;
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
public class OrganizationConsumerService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private final UtilHelper utilHelper = UtilHelper.getInstance();

    private final String organizationTopicName;
    private final String organizationOutputTopicName;
    private final OrganizationRepository organizationRepository;

    /**
     * @param streamsBuilder
     * From incoming Organization related topic capture the changes
     * Compute the aggregated Organization data
     * Publish the Organization data to aggregated Kafka output topic
     * Steps:
     * 1. Capture the `Organization` topic as Kafka Stream - KStream<String,String>
     * 2. Transform to KStream<String, Organization>
     * 3. For each Organization call the `sp_organization_event` stored proc &
     *      transform the results to KStream<String,List<Organization>
     * 4. Transform the above result to KStream<Organization.UID,Organization>
     * 5. Write the consolidated `Organization` data to aggregated `Organization` Kafka topic
     */

    public void processOrganizationData(StreamsBuilder streamsBuilder) {
        KStream<String, OrganizationOP> organizationKStream
                = streamsBuilder.stream(organizationTopicName, Consumed.with(STRING_SERDE, STRING_SERDE))
                .map((k, v) -> new KeyValue<>(
                        k,
                        utilHelper.deserializePayload(v, "/payload/after", OrganizationOP.class)))
                // KStream<String, Organization>
                .peek((key, value) -> log.info("Calling the Organization Repository for " + value.getOrganizationUid()));

        KStream<String, OrganizationOP> consolidatedOrganizationStream = organizationKStream
                .mapValues(v -> organizationRepository.computeAllOrganizations(v.getOrganizationUid()))
                //KStream<String, List<Organization>>
                .flatMap((k, v) -> v.stream()
                        .map(p -> KeyValue.pair(p.getOrganizationUid(), p))
                        .collect(Collectors.toSet()))
                // KStream<String, Organization>
                .peek((key, value) -> log.info("OrganizationInfo : {}", value.toString()));

        consolidatedOrganizationStream.to(organizationOutputTopicName,
                Produced.with(Serdes.String(), StreamsSerdes.OrganizationSerde()));
    }
}


