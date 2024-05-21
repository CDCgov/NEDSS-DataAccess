package gov.cdc.etldatapipeline.investigation;

import gov.cdc.etldatapipeline.investigation.repository.model.dto.*;
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

import static gov.cdc.etldatapipeline.commonutil.TestUtils.readFileData;
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
}
