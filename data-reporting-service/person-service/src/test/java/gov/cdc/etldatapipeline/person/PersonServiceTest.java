package gov.cdc.etldatapipeline.person;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.person.model.avro.DataEnvelope;
import gov.cdc.etldatapipeline.person.model.dto.PersonExtendedProps;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientElasticSearch;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientReporting;
import gov.cdc.etldatapipeline.person.model.dto.patient.PatientSp;
import gov.cdc.etldatapipeline.person.model.dto.persondetail.*;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderReporting;
import gov.cdc.etldatapipeline.person.model.dto.provider.ProviderSp;
import gov.cdc.etldatapipeline.person.repository.PatientRepository;
import gov.cdc.etldatapipeline.person.repository.ProviderRepository;
import gov.cdc.etldatapipeline.person.service.PersonService;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static gov.cdc.etldatapipeline.person.TestUtils.readFileData;
import static org.mockito.ArgumentMatchers.anyString;

@ExtendWith(MockitoExtension.class)
public class PersonServiceTest {

    @Mock
    PatientRepository patientRepository;

    @Mock
    ProviderRepository providerRepository;

    private final String personTopic = "PersonTopic";
    private final String patientElasticTopic = "PatientElasticTopic";
    private final String patientReportingTopic = "PatientReportingTopic";
    private final String providerElasticTopic = "ProviderElasticTopic";
    private final String providerReportingTopic = "ProviderReportingTopic";
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Test
    public void processPatientReportingData() {
        PatientSp patientSp = constructPatient();
        Mockito.when(patientRepository.computePatients(anyString())).thenReturn(List.of(patientSp));
        //Build expected unflattened Provider
        PatientReporting expectedPf = PatientReporting.build(patientSp);
        //Construct transformed patient
        constructPatPrvFull(expectedPf);

        // Validate Patient Reporting Data Transformation
        validateDataTransformation(
                readFileData("rawDataFiles/person/PersonPatientChangeData.json"),
                personTopic,
                patientReportingTopic,
                "rawDataFiles/patient/PatientReporting.json",
                "rawDataFiles/patient/PatientKey.json");
    }

    @Test
    public void processPatientElasticSearchData() {
        PatientSp patientSp = constructPatient();
        Mockito.when(patientRepository.computePatients(anyString())).thenReturn(List.of(patientSp));
        //Build expected unflattened Provider
        PatientElasticSearch expectedPf = PatientElasticSearch.build(patientSp);
        //Construct transformed patient
        constructPatPrvFull(expectedPf);

        // Validate Patient ElasticSearch Data Transformation
        validateDataTransformation(
                readFileData("rawDataFiles/person/PersonPatientChangeData.json"),
                personTopic,
                patientElasticTopic,
                "rawDataFiles/patient/PatientElastic.json",
                "rawDataFiles/patient/PatientKey.json");
    }

    @Test
    public void processProviderReportingData() {
        ProviderSp constructedProviderSp = constructProvider();
        Mockito.when(providerRepository.computeProviders(anyString())).thenReturn(List.of(constructedProviderSp));

        //Build expected unflattened Provider
        ProviderReporting expectedPf = ProviderReporting.build(constructedProviderSp);
        //Augment Provider with the flattened data
        constructPatPrvFull(expectedPf);

        // Validate Patient Reporting Data Transformation
        validateDataTransformation(
                readFileData("rawDataFiles/person/PersonProviderChangeData.json"),
                personTopic,
                providerReportingTopic,
                "rawDataFiles/provider/ProviderReporting.json",
                "rawDataFiles/provider/ProviderKey.json");
    }

    @Test
    public void processProviderElasticSearchData() {
        ProviderSp constructedProviderSp = constructProvider();
        Mockito.when(providerRepository.computeProviders(anyString())).thenReturn(List.of(constructedProviderSp));

        //Build expected unflattened Provider
        ProviderReporting expectedPf = ProviderReporting.build(constructedProviderSp);
        //Augment Provider with the flattened data
        constructPatPrvFull(expectedPf);

        // Validate Patient Reporting Data Transformation
        validateDataTransformation(
                readFileData("rawDataFiles/person/PersonProviderChangeData.json"),
                personTopic,
                providerElasticTopic,
                "rawDataFiles/provider/ProviderElasticSearch.json",
                "rawDataFiles/provider/ProviderKey.json");
    }

    /**
     * Create a mock Kafka cluster and do stream processing of the Patient/Provider data
     *
     * @param incomingChangeData    Debezium Change Data
     * @param inputTopicName        Input Topic to monitor
     * @param outputTopicName       Output Topic to produce the transformed data
     * @param expectedValueFilePath Expected transformed Json Value Data in the DataEnvelope format
     * @param expectedKeyFilePath   Expected transformed Json Key Data in the DataEnvelope format
     */
    private void validateDataTransformation(
            String incomingChangeData,
            String inputTopicName,
            String outputTopicName,
            String expectedValueFilePath,
            String expectedKeyFilePath) {
        PersonService ks = getKafkaStreamService();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        ks.processMessage(streamsBuilder);
        Topology topology = streamsBuilder.build();
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
            TestInputTopic<String, String> inputTopic = topologyTestDriver
                    .createInputTopic(inputTopicName, new StringSerializer(), new StringSerializer());

            TestOutputTopic<String, String> outputTopic = topologyTestDriver
                    .createOutputTopic(outputTopicName, new StringDeserializer(), new StringDeserializer());
            inputTopic.pipeInput("10000001", incomingChangeData);
            List<KeyValue<String, String>> actualData = outputTopic.readKeyValuesToList();
            Assertions.assertNotNull(actualData);

            //Validate the Provider Payload
            TypeReference<DataEnvelope> dataEnvelopeTypeReference = new TypeReference<>() {
            };

            DataEnvelope<DataEnvelope> actualValue
                    = objectMapper.readValue(actualData.get(0).value, dataEnvelopeTypeReference);
            Assertions.assertEquals(
                    objectMapper.readValue(TestUtils.readFileData(expectedValueFilePath), dataEnvelopeTypeReference),
                    actualValue);

            //Validate the Patient Key
            DataEnvelope<DataEnvelope> actualKey
                    = objectMapper.readValue(actualData.get(0).key, dataEnvelopeTypeReference);
            //Construct expected Patient Key
            Assertions.assertEquals(
                    objectMapper.readValue(TestUtils.readFileData(expectedKeyFilePath), dataEnvelopeTypeReference),
                    actualKey);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private PersonService getKafkaStreamService() {
        PersonService ks = new PersonService(patientRepository, providerRepository);
        ks.setPersonTopicName(personTopic);
        ks.setPatientElasticSearchTopicName(patientElasticTopic);
        ks.setPatientReportingOutputTopic(patientReportingTopic);
        ks.setProviderReportingOutputTopic(providerReportingTopic);
        ks.setProviderElasticSearchOutputTopic(providerElasticTopic);
        return ks;
    }

    private PatientSp constructPatient() {
        String filePathPrefix = "rawDataFiles/person/";
        return PatientSp.builder()
                .personUid(10000001L)
                .nameNested(readFileData(filePathPrefix + "PersonName.json"))
                .addressNested(readFileData(filePathPrefix + "PersonAddress.json"))
                .raceNested(readFileData(filePathPrefix + "PersonRace.json"))
                .telephoneNested(readFileData(filePathPrefix + "PersonTelephone.json"))
                .entityDataNested(readFileData(filePathPrefix + "PersonEntityData.json"))
                .emailNested(readFileData(filePathPrefix + "PersonEmail.json"))
                .build();
    }

    private ProviderSp constructProvider() {
        String filePathPrefix = "rawDataFiles/person/";
        return ProviderSp.builder()
                .personUid(10000001L)
                .nameNested(readFileData(filePathPrefix + "PersonName.json"))
                .addressNested(readFileData(filePathPrefix + "PersonAddress.json"))
                .telephoneNested(readFileData(filePathPrefix + "PersonTelephone.json"))
                .entityDataNested(readFileData(filePathPrefix + "PersonEntityData.json"))
                .emailNested(readFileData(filePathPrefix + "PersonEmail.json"))
                .build();
    }

    private <T extends PersonExtendedProps> void constructPatPrvFull(T patProv) {
        // Name
        Name.builder()
                .lastNm("Singgh")
                .middleNm("Js")
                .firstNm("Suurma")
                .nmSuffix("Jr")
                .build().updatePerson(patProv);

        // Address
        Address.builder()
                .streetAddr1("123 Main St.")
                .streetAddr2("")
                .city("Atlanta")
                .zip("30025")
                .cntyCd("13135")
                .county("Gwinnett County")
                .state("13")
                .stateDesc("Georgia")
                .cntryCd("840")
                .homeCountry("United States")
                .birthCountry("Canada")
                .build()
                .updatePerson(patProv);


        // Work Phone
        Phone.builder().telephoneNbr("2323222422").extensionTxt("232").cd("WP").build().updatePerson(patProv);

        // Home Phone
        Phone.builder().telephoneNbr("4562323222").extensionTxt("211").cd("H").build().updatePerson(patProv);

        // Cell Phone
        Phone.builder().telephoneNbr("2823252423").cd("CP").build().updatePerson(patProv);

        // Race
        Race.builder()
                .raceCd("2028-9")
                .raceCategoryCd("2028-9")
                .raceDescTxt("Amer Indian")
                .build()
                .updatePerson(patProv);


        // SSN
        EntityData.builder()
                .rootExtensionTxt("313431144414")
                .assigningAuthorityCd("SSA")
                .build()
                .updatePerson(patProv);


        // Patient Number
        EntityData.builder()
                .typeCd("PN")
                .rootExtensionTxt("56743114514")
                .assigningAuthorityCd("2.16.740.1.113883.3.1147.1.1002")
                .build()
                .updatePerson(patProv);

        //QEC
        EntityData.builder()
                .typeCd("QEC")
                .rootExtensionTxt("12314286")
                .build()
                .updatePerson(patProv);

        //RegNum
        EntityData.builder()
                .typeCd("PRN")
                .rootExtensionTxt("86741517517")
                .assigningAuthorityCd("3.16.740.1.113883.3.1147.1.1002")
                .build()
                .updatePerson(patProv);

        // Email
        Email.builder().emailAddress("someone2@email.com").build().updatePerson(patProv);

    }

}
