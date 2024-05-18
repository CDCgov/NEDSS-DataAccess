package gov.cdc.etldatapipeline.investigation;

import gov.cdc.etldatapipeline.investigation.repository.model.dto.*;
import gov.cdc.etldatapipeline.investigation.repository.model.reporting.InvestigationReporting;
import gov.cdc.etldatapipeline.investigation.util.ProcessInvestigationDataUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.modelmapper.ModelMapper;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static gov.cdc.etldatapipeline.investigation.TestUtils.readFileData;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class InvestigationDataProcessingTests {
    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Captor
    private ArgumentCaptor<String> topicCaptor;

    @Captor
    private ArgumentCaptor<String> keyCaptor;

    @Captor
    private ArgumentCaptor<String> messageCaptor;

    private static final String FILE_PREFIX = "rawDataFiles/";
    private static final String CONFIRMATION_TOPIC = "confirmationTopic";
    private static final String OBSERVATION_TOPIC = "observationTopic";
    private static final String NOTIFICATION_TOPIC = "notificationTopic";
    private static final Long investigationUid = 234567890L;

    ProcessInvestigationDataUtil transformer;
    private final ModelMapper modelMapper = new ModelMapper();

    BiFunction<String, List<String>, Boolean> containsWords = (input, words) ->
            words.stream().allMatch(input::contains);

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        transformer = new ProcessInvestigationDataUtil(kafkaTemplate);
    }

    @Test
    public void testConfirmationMethod() {
        Investigation investigation = new Investigation();

        investigation.setPublicHealthCaseUid(investigationUid);
        investigation.setInvestigationConfirmationMethod(readFileData(FILE_PREFIX + "ConfirmationMethod.json"));
        transformer.investigationConfirmationOutputTopicName = CONFIRMATION_TOPIC;

        InvestigationConfirmationMethodKey confirmationMethodKey = new InvestigationConfirmationMethodKey();
        confirmationMethodKey.setPublicHealthCaseUid(investigationUid);
        confirmationMethodKey.setConfirmationMethodCd("LD");

        InvestigationConfirmationMethod confirmationMethod = new InvestigationConfirmationMethod();
        confirmationMethod.setPublicHealthCaseUid(investigationUid);
        confirmationMethod.setConfirmationMethodCd("LD");
        confirmationMethod.setConfirmationMethodDescTxt("Laboratory confirmed");
        confirmationMethod.setConfirmationMethodTime("2024-01-15T10:20:57.647");

        transformer.transformInvestigationData(investigation);
        verify(kafkaTemplate, times(2)).send(topicCaptor.capture(), keyCaptor.capture(), messageCaptor.capture());
        assertEquals(CONFIRMATION_TOPIC, topicCaptor.getValue());

        Function<InvestigationConfirmationMethod, List<String>> cmDetailsFn = (m) -> Arrays.asList(
                String.valueOf(m.getPublicHealthCaseUid()),
                m.getConfirmationMethodCd(),
                m.getConfirmationMethodDescTxt(),
                m.getConfirmationMethodTime());

        Function<InvestigationConfirmationMethodKey, List<String>> cmKeyFn = (k) -> Arrays.asList(
                String.valueOf(k.getPublicHealthCaseUid()),
                k.getConfirmationMethodCd());

        assertTrue(containsWords.apply(keyCaptor.getValue(), cmKeyFn.apply(confirmationMethodKey)));
        assertTrue(containsWords.apply(messageCaptor.getValue(), cmDetailsFn.apply(confirmationMethod)));
    }

    @Test
    public void testObservationNotificationIds() {
        Investigation investigation = new Investigation();

        investigation.setPublicHealthCaseUid(investigationUid);
        investigation.setObservationNotificationIds(readFileData(FILE_PREFIX + "ObservationNotificationIds.json"));
        transformer.investigationNotificationOutputTopicName = NOTIFICATION_TOPIC;
        transformer.investigationObservationOutputTopicName = OBSERVATION_TOPIC;

        InvestigationNotification notification = new InvestigationNotification();
        notification.setPublicHealthCaseUid(investigationUid);
        notification.setNotificationId(263748597L);

        InvestigationObservation observation = new InvestigationObservation();
        observation.setPublicHealthCaseUid(investigationUid);
        observation.setObservationId(263748596L);

        transformer.transformInvestigationData(investigation);
        verify(kafkaTemplate, times (2)).send(topicCaptor.capture(), keyCaptor.capture(), messageCaptor.capture());
        assertEquals(OBSERVATION_TOPIC, topicCaptor.getValue());

        Function<InvestigationNotification, List<String>> nDetailsFn = (n) -> Arrays.asList(
                String.valueOf(n.getPublicHealthCaseUid()),
                String.valueOf(n.getNotificationId()));

        Function<InvestigationObservation, List<String>> oDetailsFn = (o) -> Arrays.asList(
                String.valueOf(o.getPublicHealthCaseUid()),
                String.valueOf(o.getObservationId()));

        String actualCombined = String.join(" ",messageCaptor.getAllValues());

        assertTrue(containsWords.apply(actualCombined, nDetailsFn.apply(notification)));
        assertTrue(containsWords.apply(actualCombined, oDetailsFn.apply(observation)));
    }

    @Test
    public void testInvestigationReporting() {
        final var investigation = getInvestigation();

        InvestigationReporting reporting = modelMapper.map(investigation, InvestigationReporting.class);

        assertEquals(investigation.getPublicHealthCaseUid(), reporting.getPublicHealthCaseUid());
        assertEquals(investigation.getProgramJurisdictionOid(), reporting.getProgramJurisdictionOid());
        assertEquals(investigation.getJurisdictionCode(), reporting.getJurisdictionCode());
        assertEquals(investigation.getJurisdictionNm(), reporting.getJurisdictionNm());
        assertEquals(investigation.getMoodCd(), reporting.getMoodCd());
        assertEquals(investigation.getClassCd(), reporting.getClassCd());
        assertEquals(investigation.getCaseTypeCd(), reporting.getCaseTypeCd());
        assertEquals(investigation.getCaseClassCd(), reporting.getCaseClassCd());
        assertEquals(investigation.getInvCaseStatus(), reporting.getInvCaseStatus());
        assertNull(reporting.getOutbreakName());
        assertEquals(investigation.getCd(), reporting.getCd());
        assertEquals(investigation.getCdDescTxt(), reporting.getCdDescTxt());
        assertEquals(investigation.getProgAreaCd(), reporting.getProgAreaCd());

        assertNull(reporting.getPregnantInd());
        assertNull(reporting.getPregnantIndCd());

        assertEquals(investigation.getLocalId(), reporting.getLocalId());
        assertEquals(investigation.getRptFormCmpltTime(), reporting.getRptFormCmpltTime());

        assertNull(reporting.getActivityToTime());
        assertNull(reporting.getActivityFromTime());

        assertEquals(investigation.getAddUserId(), reporting.getAddUserId());
        assertEquals(investigation.getAddUserName(), reporting.getAddUserName());
        assertEquals(investigation.getAddTime(), reporting.getAddTime());

        assertNull(reporting.getLastChgTime());
        assertNull(reporting.getLastChgUserId());
        assertNull(reporting.getLastChgUserName());
        assertNull(reporting.getCurrProcessState());
        assertNull(reporting.getCurrProcessStateCd());

        assertEquals(investigation.getInvestigationStatusCd(), reporting.getInvestigationStatusCd());
        assertEquals(investigation.getInvestigationStatus(), reporting.getInvestigationStatus());
        assertEquals(investigation.getRecordStatusCd(), reporting.getRecordStatusCd());
        assertEquals(investigation.getSharedInd(), reporting.getSharedInd());

        assertNull(reporting.getTxt());
        assertNull(reporting.getEffectiveFromTime());
        assertNull(reporting.getEffectiveToTime());
        assertNull(reporting.getEffectiveDurationAmt());
        assertNull(reporting.getEffectiveDurationUnitCd());

        assertEquals(investigation.getRptSourceCd(), reporting.getRptSourceCd());
        assertEquals(investigation.getRptSrcCdDesc(), reporting.getRptSrcCdDesc());
        assertEquals(investigation.getMmwrWeek(), reporting.getMmwrWeek());
        assertEquals(investigation.getMmwrYear(), reporting.getMmwrYear());

        assertNull(reporting.getDiseaseImportedCd());
        assertNull(reporting.getDiseaseImportedInd());
        assertNull(reporting.getImportedCountryCd());
        assertNull(reporting.getImportedStateCd());
        assertNull(reporting.getImportedCountyCd());
        assertNull(reporting.getImportedFromCountry());
        assertNull(reporting.getImportedFromState());
        assertNull(reporting.getImportedFromCounty());
        assertNull(reporting.getImportedCityDescTxt());

        assertEquals(investigation.getDiagnosisTime(), reporting.getDiagnosisTime());

        assertNull(reporting.getHospitalizedAdminTime());
        assertNull(reporting.getHospitalizedDischargeTime());
        assertNull(reporting.getHospitalizedDurationAmt());
        assertNull(reporting.getHospitalizedIndCd());
        assertNull(reporting.getHospitalizedInd());
        assertNull(reporting.getOutbreakInd());
        assertNull(reporting.getOutbreakIndVal());
        assertNull(reporting.getTransmissionModeCd());
        assertNull(reporting.getTransmissionMode());
        assertNull(reporting.getOutcomeCd());
        assertNull(reporting.getDieFrmThisIllnessInd());
        assertNull(reporting.getDayCareInd());
        assertNull(reporting.getFoodHandlerIndCd());
        assertNull(reporting.getFoodHandlerInd());
        assertNull(reporting.getDeceasedTime());

        assertEquals(investigation.getPatAgeAtOnset(), reporting.getPatAgeAtOnset());
        assertEquals(investigation.getPatAgeAtOnsetUnitCd(), reporting.getPatAgeAtOnsetUnitCd());
        assertEquals(investigation.getPatAgeAtOnsetUnit(), reporting.getPatAgeAtOnsetUnit());

        assertEquals(investigation.getDetectionMethodDescTxt(), reporting.getDetectionMethodDescTxt());
        assertEquals(investigation.getProgramAreaDescription(), reporting.getProgramAreaDescription());

        assertNull(reporting.getContactInvPriority());
        assertNull(reporting.getContactInvStatus());
        assertNull(reporting.getReferralBasisCd());
        assertNull(reporting.getReferralBasis());
        assertNull(reporting.getInvPriorityCd());

        assertNull(reporting.getNotificationLocalId());
        assertNull(reporting.getNotificationAddTime());
        assertNull(reporting.getNotificationRecordStatusCd());
        assertNull(reporting.getNotificationLastChgTime());
    }

    private Investigation getInvestigation() {
        Investigation investigation = new Investigation();

        investigation.setPublicHealthCaseUid(investigationUid);
        investigation.setPublicHealthCaseUid(1L);
        investigation.setProgramJurisdictionOid(2L);
        investigation.setJurisdictionCode("333");
        investigation.setJurisdictionNm("Nevarro County");

        investigation.setMoodCd("EVN");
        investigation.setClassCd("CASE");
        investigation.setCaseTypeCd("I");
        investigation.setCaseClassCd("C");
        investigation.setInvCaseStatus("Confirmed");
        investigation.setCd("112233");
        investigation.setCdDescTxt("Testaphobia Syndrome");

        investigation.setProgAreaCd("PROG");
        investigation.setLocalId("CAS1000000AA01");
        investigation.setRptFormCmpltTime("2020-02-20T00:00:00.000");

        investigation.setAddUserId(2020L);
        investigation.setAddUserName("ODS NBS");
        investigation.setAddTime("2020-02-20T00:20:00.000");

        investigation.setInvestigationStatusCd("O");
        investigation.setInvestigationStatus("Open");
        investigation.setRecordStatusCd("ACTIVE");
        investigation.setSharedInd("T");

        investigation.setRptSourceCd("LA");
        investigation.setRptSrcCdDesc("Laboratory");
        investigation.setMmwrWeek("20");
        investigation.setMmwrYear("2020");

        investigation.setDiagnosisTime("2020-02-20T00:00:00.000");
        investigation.setPatAgeAtOnset("22");
        investigation.setPatAgeAtOnsetUnitCd("Y");
        investigation.setPatAgeAtOnsetUnit("Years");
        investigation.setDetectionMethodDescTxt("Self-referral");
        investigation.setProgramAreaDescription("General");

        return investigation;
    }
}
