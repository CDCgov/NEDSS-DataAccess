package gov.cdc.etldatapipeline.changedata.service;

import gov.cdc.etldatapipeline.changedata.repository.PageRepository;
import gov.cdc.etldatapipeline.changedata.repository.PatientRepository;
import gov.cdc.etldatapipeline.changedata.repository.ProviderRepository;
import gov.cdc.etldatapipeline.changedata.repository.OrganizationRepository;
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
    private final PageRepository pageRepository;
    private final ProviderRepository providerRepository;
    private final OrganizationRepository organizationRepository;
    @Value("#{kafkaConfig.getNbsPagesTopicName()}")
    private String nbsPagesTopicName;
    @Value("#{kafkaConfig.getProviderTopicName()}")
    private String providerTopicName;
    @Value("#{kafkaConfig.getProviderAggregateTopicName()}")
    private String providerOutputTopicName;
    @Value("#{kafkaConfig.getPersonTopicName()}")
    private String personTopicName;
    @Value("#{kafkaConfig.getParticipationTopicName()}")
    private String participationTopicName;
    @Value("#{kafkaConfig.getCtContactTopicName()}")
    private String ctContactTopicName;
    @Value("#{kafkaConfig.getOrganizationTopicName()}")
    private String organizationTopicName;
    @Value("#{kafkaConfig.getOrganizationAggregateTopicName()}")
    private String organizationOutputTopicName;

    @Autowired
    public void processMessage(StreamsBuilder streamsBuilder) {

        ProviderService providerService = new ProviderService(
                personTopicName,
                providerOutputTopicName,
                providerRepository);
        providerService.processProviderData(streamsBuilder);

        OrganizationService organizationService = new OrganizationService(
                organizationTopicName,
                organizationOutputTopicName,
                organizationRepository);
        organizationService.processOrganizationData(streamsBuilder);

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
