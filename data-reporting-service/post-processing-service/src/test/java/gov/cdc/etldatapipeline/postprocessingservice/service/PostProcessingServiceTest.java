package gov.cdc.etldatapipeline.postprocessingservice.service;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import gov.cdc.etldatapipeline.postprocessingservice.repository.OrganizationRepository;
import gov.cdc.etldatapipeline.postprocessingservice.repository.PatientRepository;
import gov.cdc.etldatapipeline.postprocessingservice.repository.ProviderRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;
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

    private ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
//    Logger logger = (Logger) LoggerFactory.getLogger(PostProcessingService.class);

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        postProcessingServiceMock = new PostProcessingService(patientRepositoryMock,
                providerRepositoryMock, organizationRepositoryMock);
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

        postProcessingServiceMock.postProcessPatientMessage(key, topic);
        postProcessingServiceMock.processCachedIds();

        String expectedPatientIdsString = "123"; //String.join(",", patientIds.stream().map(String::valueOf).collect(Collectors.toList()));
        verify(patientRepositoryMock).executeStoredProcForPatientIds(expectedPatientIdsString);
        verify(patientRepositoryMock, times(1)).executeStoredProcForPatientIds("123");
        assertTrue(postProcessingServiceMock.idCache.containsKey(topic));

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(3, logs.size());
        assertTrue(logs.get(2).getMessage().contains("Stored proc execution completed."));
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

        postProcessingServiceMock.postProcessProviderMessage(key, topic);
        postProcessingServiceMock.processCachedIds();

        String expectedProviderIdsString = "123"; //String.join(",", providerIds.stream().map(String::valueOf).collect(Collectors.toList()));
        verify(providerRepositoryMock).executeStoredProcForProviderIds(expectedProviderIdsString);
        verify(providerRepositoryMock, times(1)).executeStoredProcForProviderIds("123");
        assertTrue(postProcessingServiceMock.idCache.containsKey(topic));

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(3, logs.size());
        assertTrue(logs.get(2).getMessage().contains("Stored proc execution completed."));
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

        postProcessingServiceMock.postProcessOrganizationMessage(key, topic);
        postProcessingServiceMock.processCachedIds();

        String expectedOrganizationIdsIdsString = "123"; //String.join(",", organizationIds.stream().map(String::valueOf).collect(Collectors.toList()));
        verify(organizationRepositoryMock).executeStoredProcForOrganizationIds(expectedOrganizationIdsIdsString);
        verify(organizationRepositoryMock, times(1)).executeStoredProcForOrganizationIds("123");
        assertTrue(postProcessingServiceMock.idCache.containsKey(topic));

        List<ILoggingEvent> logs = listAppender.list;
        assertEquals(3, logs.size());
        assertTrue(logs.get(2).getMessage().contains("Stored proc execution completed."));
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
        String messageKey = "invalid_key";
        String topic = "dummy_topic";

        assertThrows(RuntimeException.class, () -> postProcessingServiceMock.extractIdFromMessage(messageKey, topic));
    }
}