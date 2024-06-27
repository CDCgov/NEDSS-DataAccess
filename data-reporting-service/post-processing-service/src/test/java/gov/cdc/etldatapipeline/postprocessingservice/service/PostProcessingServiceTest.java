package gov.cdc.etldatapipeline.postprocessingservice.service;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import gov.cdc.etldatapipeline.postprocessingservice.repository.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PostProcessingServiceTest {

    @InjectMocks
    private PostProcessingService postProcessingServiceMock;
    @Mock
    private PatientRepository patientRepositoryMock;
    @Mock
    private ProviderRepository providerRepositoryMock;
    @Mock
    private OrganizationRepository organizationRepositoryMock;
    @Mock
    private InvestigationRepository investigationRepositoryMock;
    @Mock
    private NotificationRepository notificationRepositoryMock;
    @Mock
    private PageBuilderRepository pageBuilderRepositoryMock;

    private final ListAppender<ILoggingEvent> listAppender = new ListAppender<>();

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        postProcessingServiceMock = new PostProcessingService(patientRepositoryMock, providerRepositoryMock,
                organizationRepositoryMock, investigationRepositoryMock, notificationRepositoryMock, pageBuilderRepositoryMock);
        Logger logger = (Logger) LoggerFactory.getLogger(PostProcessingService.class);
        listAppender.start();
        logger.addAppender(listAppender);
    }

    @AfterEach
    public void tearDown() {
        Logger logger = (Logger) LoggerFactory.getLogger(PostProcessingService.class);
        logger.detachAppender(listAppender);
    }

    @Test
    public void testPostProcessPatientMessage() {
        String key = "{\"payload\":{\"patient_uid\":123}}";
        String topic = "dummy_patient";

        postProcessingServiceMock.postProcessMessage(key, topic);
        postProcessingServiceMock.processCachedIds();

        String expectedPatientIdsString = "123";
        verify(patientRepositoryMock).executeStoredProcForPatientIds(expectedPatientIdsString);
        assertTrue(postProcessingServiceMock.idCache.containsKey(topic));

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(3, logs.size());
        assertTrue(logs.get(2).getMessage().contains(PostProcessingService.SP_EXECUTION_COMPLETED));
    }

    @Test
    public void testExtractIdFromPatientMessage() {
        String messageKey = "{\"payload\":{\"patient_uid\":123}}";
        String topic = "dummy_patient";
        Long expectedId = 123L;
        Long extractedId = postProcessingServiceMock.extractIdFromMessage(messageKey, topic);

        assertEquals(expectedId, extractedId);
    }

    @Test
    public void testPostProcessProviderMessage() {
        String key = "{\"payload\":{\"provider_uid\":123}}";
        String topic = "dummy_provider";

        postProcessingServiceMock.postProcessMessage(key, topic);
        postProcessingServiceMock.processCachedIds();

        String expectedProviderIdsString = "123";
        verify(providerRepositoryMock).executeStoredProcForProviderIds(expectedProviderIdsString);
        assertTrue(postProcessingServiceMock.idCache.containsKey(topic));

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(3, logs.size());
        assertTrue(logs.get(2).getMessage().contains(PostProcessingService.SP_EXECUTION_COMPLETED));
    }

    @Test
    public void testExtractIdFromProviderMessage() {
        String messageKey = "{\"payload\":{\"provider_uid\":123}}";
        String topic = "dummy_provider";
        Long expectedId = 123L;
        Long extractedId = postProcessingServiceMock.extractIdFromMessage(messageKey, topic);

        assertEquals(expectedId, extractedId);
    }

    @Test
    public void testPostProcessOrganizationMessage() {
        String key = "{\"payload\":{\"organization_uid\":123}}";
        String topic = "dummy_organization";

        postProcessingServiceMock.postProcessMessage(key, topic);
        postProcessingServiceMock.processCachedIds();

        String expectedOrganizationIdsIdsString = "123";
        verify(organizationRepositoryMock).executeStoredProcForOrganizationIds(expectedOrganizationIdsIdsString);
        assertTrue(postProcessingServiceMock.idCache.containsKey(topic));

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(3, logs.size());
        assertTrue(logs.get(2).getMessage().contains(PostProcessingService.SP_EXECUTION_COMPLETED));
    }

    @Test
    public void testExtractIdFromOrganizationMessage() {
        String messageKey = "{\"payload\":{\"organization_uid\":123}}";
        String topic = "dummy_organization";
        Long expectedId = 123L;
        Long extractedId = postProcessingServiceMock.extractIdFromMessage(messageKey, topic);

        assertEquals(expectedId, extractedId);
    }

    @Test
    public void testPostProcessInvestigationMessage() {
        String key = "{\"payload\":{\"public_health_case_uid\":123}}";
        String topic = "dummy_investigation";

        postProcessingServiceMock.postProcessMessage(key, topic);
        postProcessingServiceMock.processCachedIds();

        String expectedPublicHealthCaseIdsString = "123";
        verify(investigationRepositoryMock).executeStoredProcForPublicHealthCaseIds(expectedPublicHealthCaseIdsString);
        verify(investigationRepositoryMock).executeStoredProcForFPageCase(expectedPublicHealthCaseIdsString);
        assertTrue(postProcessingServiceMock.idCache.containsKey(topic));

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(5, logs.size());
        assertTrue(logs.get(2).getMessage().contains(PostProcessingService.SP_EXECUTION_COMPLETED));
    }

    @Test
    public void testExtractIdFromInvestigationMessage() {
        String messageKey = "{\"payload\":{\"public_health_case_uid\":123}}";
        String topic = "dummy_investigation";
        Long expectedId = 123L;
        Long extractedId = postProcessingServiceMock.extractIdFromMessage(messageKey, topic);

        assertEquals(expectedId, extractedId);
    }

    @Test
    public void testPostProcessNotificationMessage() {
        String key = "{\"payload\":{\"notification_uid\":123}}";
        String topic = "dummy_notifications";

        postProcessingServiceMock.postProcessMessage(key, topic);
        postProcessingServiceMock.processCachedIds();

        String expectedNotificationIdsString = "123";
        verify(notificationRepositoryMock).executeStoredProcForNotificationIds(expectedNotificationIdsString);
        assertTrue(postProcessingServiceMock.idCache.containsKey(topic));

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(3, logs.size());
        assertTrue(logs.get(2).getMessage().contains(PostProcessingService.SP_EXECUTION_COMPLETED));
    }

    @Test
    public void testExtractIdFromNotificationMessage() {
        String messageKey = "{\"payload\":{\"notification_uid\":123}}";
        String topic = "dummy_notifications";
        Long expectedId = 123L;
        Long extractedId = postProcessingServiceMock.extractIdFromMessage(messageKey, topic);

        assertEquals(expectedId, extractedId);
    }

    @Test
    public void testPostProcessPageBuilder() {
        String key = "{\"payload\":{\"public_health_case_uid\":123, \"rdb_table_name_list\":\"D_INV_CLINICAL,D_INV_ADMINISTRATIVE\"}}";
        String topic = "dummy_investigation";

        postProcessingServiceMock.postProcessMessage(key, topic);
        postProcessingServiceMock.processCachedIds();

        Long expectedPublicHealthCaseId = 123L;
        String expectedRdbTableNames = "D_INV_CLINICAL,D_INV_ADMINISTRATIVE";
        verify(pageBuilderRepositoryMock).executeStoredProcForPageBuilder(expectedPublicHealthCaseId, expectedRdbTableNames);
        assertTrue(postProcessingServiceMock.idVals.containsKey(expectedPublicHealthCaseId));
        assertTrue(postProcessingServiceMock.idVals.containsValue(expectedRdbTableNames));

        String expectedMsgInv = "Processing the investigation message topic: " + topic + ". Calling stored proc: sp_nrt_investigation_postprocessing('123')";
        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(7, logs.size());
        assertTrue(logs.get(6).getMessage().contains(PostProcessingService.SP_EXECUTION_COMPLETED));
    }

    @Test
    public void testPostProcessMultipleMessages() {
        String orgKey1 = "{\"payload\":{\"organization_uid\":123}}";
        String orgKey2 = "{\"payload\":{\"organization_uid\":124}}";
        String orgTopic = "dummy_organization";

        String ntfKey1 = "{\"payload\":{\"notification_uid\":234}}";
        String ntfKey2 = "{\"payload\":{\"notification_uid\":235}}";
        String ntfTopic = "dummy_notifications";

        postProcessingServiceMock.postProcessMessage(orgKey1, orgTopic);
        postProcessingServiceMock.postProcessMessage(orgKey2, orgTopic);
        postProcessingServiceMock.postProcessMessage(ntfKey1, ntfTopic);
        postProcessingServiceMock.postProcessMessage(ntfKey2, ntfTopic);

        postProcessingServiceMock.processCachedIds();

        String expectedOrganizationIdsIdsString = "123,124";
        String expectedNotificationIdsString = "234,235";

        verify(organizationRepositoryMock).executeStoredProcForOrganizationIds(expectedOrganizationIdsIdsString);
        assertTrue(postProcessingServiceMock.idCache.containsKey(orgTopic));

        verify(notificationRepositoryMock).executeStoredProcForNotificationIds(expectedNotificationIdsString);
        verify(notificationRepositoryMock, times(1)).executeStoredProcForNotificationIds(expectedNotificationIdsString);
        assertTrue(postProcessingServiceMock.idCache.containsKey(ntfTopic));
    }

    @Test
    public void testProcessMessageEmptyCache() {
        String topic = "dummy_patient";

        postProcessingServiceMock.idCache.put(topic, new CopyOnWriteArrayList<>());
        postProcessingServiceMock.processCachedIds();

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(1, logs.size());
        assertTrue(logs.get(0).getMessage().contains("No ids to process from the topics."));
    }

    @Test
    public void testExtractIdFromMessageException() {
        String invalidKey = "invalid_key";
        String invalidTopic = "dummy_topic";

        assertThrows(RuntimeException.class, () -> postProcessingServiceMock.extractIdFromMessage(invalidKey, invalidTopic));
    }
}