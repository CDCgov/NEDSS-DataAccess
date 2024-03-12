package gov.cdc.datareportingservice.changedata.service;

import gov.cdc.datareportingservice.changedata.repository.PatientRepository;
import gov.cdc.datareportingservice.changedata.repository.ProviderRepository;
import gov.cdc.datareportingservice.changedata.repository.OrganizationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaStreamsService {

    private final PatientRepository patientRepository;
    private final ProviderRepository providerRepository;
    private final OrganizationRepository organizationRepository;
    @Value("#{kafkaConfig.getProviderAggregateTopicName()}")
    private String providerOutputTopicName;
    @Value("#{kafkaConfig.getPersonTopicName()}")
    private String personTopicName;
    @Value("#{kafkaConfig.getPatientAggregateTopicName()}")
    private String patientOutputTopicName;
    @Value("#{kafkaConfig.getOrganizationTopicName()}")
    private String organizationTopicName;
    @Value("#{kafkaConfig.getOrganizationAggregateTopicName()}")
    private String organizationOutputTopicName;

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {

        ProviderConsumerService providerConsumerService = new ProviderConsumerService(
                personTopicName,
                providerOutputTopicName,
                providerRepository);
        providerConsumerService.processProviderData(streamsBuilder);

        OrganizationConsumerService organizationConsumerService = new OrganizationConsumerService(
                organizationTopicName,
                organizationOutputTopicName,
                organizationRepository);
        organizationConsumerService.processOrganizationData(streamsBuilder);

        PatientConsumerService patientService = new PatientConsumerService(
                personTopicName,
                patientOutputTopicName,
                patientRepository);
        patientService.processPatientData(streamsBuilder);

             /* PatientService patientService = new PatientService(
                personTopicName,
                participationTopicName,
                ctContactTopicName,
                patientRepository);
        patientService.processPatientData(streamsBuilder);*/

        /*NbsPageService pageService = new NbsPageService(
                nbsPagesTopicName,
                pageRepository);
        pageService.processNbsPage(streamsBuilder);*/
    }
}
