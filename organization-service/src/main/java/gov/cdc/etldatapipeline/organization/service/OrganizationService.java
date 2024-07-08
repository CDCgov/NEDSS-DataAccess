package gov.cdc.etldatapipeline.organization.service;

import gov.cdc.etldatapipeline.organization.model.dto.org.OrganizationSp;
import gov.cdc.etldatapipeline.organization.model.odse.Organization;
import gov.cdc.etldatapipeline.organization.repository.OrgRepository;
import gov.cdc.etldatapipeline.organization.transformer.OrganizationTransformers;
import gov.cdc.etldatapipeline.organization.transformer.OrganizationType;
import gov.cdc.etldatapipeline.organization.utils.UtilHelper;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.Set;


@Service
@Setter
@Slf4j
public class OrganizationService {
    @Value("${spring.kafka.output.organizationElastic.topic-name}")
    private String orgElasticSearchTopic;

    @Value("${spring.kafka.output.organizationReporting.topic-name}")
    private String orgReportingOutputTopic;

    private final OrgRepository orgRepository;
    private final OrganizationTransformers transformer;

    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public OrganizationService(OrgRepository orgRepository, OrganizationTransformers transformer, KafkaTemplate<String,String> kafkaTemplate) {
        this.orgRepository = orgRepository;
        this.transformer = transformer;
        this.kafkaTemplate = kafkaTemplate;
    }

    @RetryableTopic(
            attempts = "${spring.kafka.consumer.max-retry}",
            autoCreateTopics = "false",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            retryTopicSuffix = "${spring.kafka.dlq.retry-suffix}",
            dltTopicSuffix = "${spring.kafka.dlq.dlq-suffix}",
            // retry topic name, such as topic-retry-1, topic-retry-2, etc
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            // time to wait before attempting to retry
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            exclude = {
                    SerializationException.class,
                    DeserializationException.class,
                    RuntimeException.class
            }
    )
    @KafkaListener(
            topics = "${spring.kafka.input.topic-name}"
    )
    public void processMessage(String message,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        try {
            Organization organization = UtilHelper.getInstance().deserializePayload(message, "/payload/after", Organization.class);
            if (organization != null) {
                log.info("Received OrganizationUid: {} from topic: {}", organization.getOrganizationUid(), topic);
                Set<OrganizationSp> organizations = orgRepository.computeAllOrganizations(organization.getOrganizationUid());

                organizations.forEach(org -> {
                    String reportingKey = transformer.buildOrganizationKey(org);
                    String reportingData = transformer.processData(org, OrganizationType.ORGANIZATION_REPORTING);
                    kafkaTemplate.send(orgReportingOutputTopic, reportingKey, reportingData);
                    log.info("Organization Reporting: {}", reportingData);

                    String elasticKey = transformer.buildOrganizationKey(org);
                    String elasticData = transformer.processData(org, OrganizationType.ORGANIZATION_ELASTIC_SEARCH);
                    kafkaTemplate.send(orgElasticSearchTopic, elasticKey, elasticData);
                    log.info("Organization Elastic: {}", elasticData!= null ? elasticData : "");
                });
            }
        } catch (Exception e) {
            log.error("Error processing organization message: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}