package gov.cdc.etldatapipeline.investigation.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.investigation.repository.model.dto.*;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

@Component
@RequiredArgsConstructor
public class ProcessInvestigationDataUtil {
    private static final Logger logger = LoggerFactory.getLogger(ProcessInvestigationDataUtil.class);

    @Value("${spring.kafka.stream.output.investigation.topic-name-confirmation}")
    public String investigationConfirmationOutputTopicName = "cdc.nbs_odse.dbo.Investigation.Confirmation";

    @Value("${spring.kafka.stream.output.investigation.topic-name-notification}")
    public String investigationNotificationOutputTopicName = "cdc.nbs_odse.dbo.Investigation.Notification";

    @Value("${spring.kafka.stream.output.investigation.topic-name-observation}")
    public String investigationObservationOutputTopicName = "cdc.nbs_odse.dbo.Investigation.output.Observation";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public InvestigationTransformed transformInvestigationData(Investigation investigation) {

        InvestigationTransformed investigationTransformed = new InvestigationTransformed();
        ObjectMapper objectMapper = new ObjectMapper();

        transformPersonParticipations(investigation.getPersonParticipations(), investigationTransformed, objectMapper);
        transformOrganizationParticipations(investigation.getOrganizationParticipations(), investigationTransformed, objectMapper);
        transformActIds(investigation.getActIds(), investigationTransformed, objectMapper);
        transformNotificationIds(investigation.getObservationNotificationIds(), objectMapper);
        transformObservationIds(investigation.getObservationNotificationIds(), investigationTransformed, objectMapper);
        transformInvestigationConfirmationMethod(investigation.getInvestigationConfirmationMethod(), investigationTransformed, objectMapper);

        return investigationTransformed;
    }


    private void transformPersonParticipations(String personParticipations, InvestigationTransformed investigationTransformed, ObjectMapper objectMapper) {
        try {
            JsonNode personParticipationsJsonArray = personParticipations != null ? objectMapper.readTree(personParticipations) : null;

            if(personParticipationsJsonArray != null && personParticipationsJsonArray.isArray()) {
                for(JsonNode node : personParticipationsJsonArray) {
                    String typeCode = node.get("type_cd").asText();
                    String subjectClassCode = node.get("subject_class_cd").asText();
                    String personCode = node.get("person_cd").asText();

                    if(typeCode.equals("InvestgrOfPHC") && subjectClassCode.equals("PSN") && personCode.equals("PRV")) {
                        investigationTransformed.setInvestigatorId(node.get("entity_id").asLong());
                    }
                    if(typeCode.equals("PhysicianOfPHC") && subjectClassCode.equals("PSN") && personCode.equals("PRV")) {
                        investigationTransformed.setPhysicianId(node.get("entity_id").asLong());
                    }
                    if(typeCode.equals("SubjOfPHC") && subjectClassCode.equals("PSN") && personCode.equals("PAT")) {
                        investigationTransformed.setPatientId(node.get("entity_id").asLong());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing Person Participation JSON array from investigation data: {}", e.getMessage());
        }
    }

    private void transformOrganizationParticipations(String organizationParticipations, InvestigationTransformed investigationTransformed, ObjectMapper objectMapper) {
        try {
            JsonNode organizationParticipationsJsonArray = organizationParticipations != null ? objectMapper.readTree(organizationParticipations) : null;

            if(organizationParticipationsJsonArray != null && organizationParticipationsJsonArray.isArray()) {
                for(JsonNode node : organizationParticipationsJsonArray) {
                    String typeCode = node.get("type_cd").asText();
                    String subjectClassCode = node.get("subject_class_cd").asText();

                    if(typeCode.equals("OrgAsReporterOfPHC") && subjectClassCode.equals("ORG")) {
                        investigationTransformed.setOrganizationId(node.get("entity_id").asLong());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing Organization Participation JSON array from investigation data: {}", e.getMessage());
        }
    }

    private void transformActIds(String actIds, InvestigationTransformed investigationTransformed, ObjectMapper objectMapper) {
        try {
            JsonNode actIdsJsonArray = actIds != null ? objectMapper.readTree(actIds) : null;

            if(actIdsJsonArray != null && actIdsJsonArray.isArray()) {
                for(JsonNode node : actIdsJsonArray) {
                    int actIdSeq = node.get("act_id_seq").asInt();
                    String typeCode = node.get("type_cd").asText();

                    if(typeCode.equals("STATE") && actIdSeq == 1) {
                        investigationTransformed.setInvStateCaseId(node.get("root_extension_txt").asText());
                    }
                    if(typeCode.equals("CITY") && actIdSeq == 2) {
                        investigationTransformed.setCityCountyCaseNbr(node.get("root_extension_txt").asText());
                    }
                    if(typeCode.equals("LEGACY") && actIdSeq == 3) {
                        investigationTransformed.setLegacyCaseId(node.get("root_extension_txt").asText());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing Act Ids JSON array from investigation data: {}", e.getMessage());
        }
    }

    private void transformNotificationIds(String observationNotificationIds, ObjectMapper objectMapper) {
        try {
            JsonNode investigationNotificationIdsJsonArray = observationNotificationIds != null ? objectMapper.readTree(observationNotificationIds) : null;
            InvestigationNotification investigationNotification = new InvestigationNotification();
            List<Long> notificationIds = new ArrayList<>();

            if(investigationNotificationIdsJsonArray != null && investigationNotificationIdsJsonArray.isArray()) {
                for(JsonNode node : investigationNotificationIdsJsonArray) {
                    String sourceClassCode = node.get("source_class_cd").asText();
                    String actTypeCode = node.get("act_type_cd").asText();

                    if(sourceClassCode.equals("NOTF") && actTypeCode.equals("Notification")) {
                        investigationNotification.setInvestigationId(node.get("public_health_case_uid").asLong());
                        notificationIds.add(node.get("source_act_uid").asLong());
                    }
                }
                for(Long id : notificationIds) {
                    investigationNotification.setNotificationId(id);
                    kafkaTemplate.send(investigationNotificationOutputTopicName, investigationNotification.toString());
                }
            }
        } catch (Exception e) {
            logger.error("Error processing Observation Notification Ids JSON array from investigation data: {}", e.getMessage());
        }
    }

    private void transformObservationIds(String observationNotificationIds, InvestigationTransformed investigationTransformed, ObjectMapper objectMapper) {
        try {
            JsonNode investigationObservationIdsJsonArray = observationNotificationIds != null ? objectMapper.readTree(observationNotificationIds) : null;
            InvestigationObservation investigationObservation = new InvestigationObservation();
            List<Long> observationIds = new ArrayList<>();

            if(investigationObservationIdsJsonArray != null && investigationObservationIdsJsonArray.isArray()) {
                for(JsonNode node : investigationObservationIdsJsonArray) {
                    String sourceClassCode = node.get("source_class_cd").asText();
                    String actTypeCode = node.get("act_type_cd").asText();

                    if(sourceClassCode.equals("OBS") && actTypeCode.equals("PHCInvForm")) {
                        investigationTransformed.setPhcInvFormId(node.get("source_act_uid").asLong());
                    }

                    if(sourceClassCode.equals("OBS") && actTypeCode.equals("LabReport")) {
                        investigationObservation.setInvestigationId(node.get("public_health_case_uid").asLong());
                        observationIds.add(node.get("source_act_uid").asLong());
                    }
                }

                for(Long id : observationIds) {
                    investigationObservation.setObservationId(id);
                    kafkaTemplate.send(investigationObservationOutputTopicName, investigationObservation.toString());
                }
            }
        } catch (Exception e) {
            logger.error("Error processing Observation Notification Ids JSON array from investigation data: {}", e.getMessage());
        }
    }

    private void transformInvestigationConfirmationMethod(String investigationConfirmationMethod, InvestigationTransformed investigationTransformed, ObjectMapper objectMapper) {
        try {
            JsonNode investigationConfirmationMethodJsonArray = investigationConfirmationMethod != null ? objectMapper.readTree(investigationConfirmationMethod) : null;
            InvestigationConfirmationMethod investigationConfirmation = new InvestigationConfirmationMethod();
            Long investigationId = null;
            Map<String, String> confirmationMethodMap = new HashMap<>();
            Instant confirmationMethodTime = null;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            // Redundant time variable in case if confirmation_method_time is null in all rows of the array
            String phcLastChgTime = null;


            if(investigationConfirmationMethodJsonArray != null && investigationConfirmationMethodJsonArray.isArray()) {
                investigationId = investigationConfirmationMethodJsonArray.get(0).get("public_health_case_uid").asLong();
                phcLastChgTime = investigationConfirmationMethodJsonArray.get(0).get("phc_last_chg_time").asText();
                for(JsonNode node : investigationConfirmationMethodJsonArray) {
                        String confirmationMethodTimeString = node.get("confirmation_method_time").asText();
                        Instant currentInstant = null;

                        // While getting data from JSON node, it is considered as literal String and that is why the null check
                        // has equals for null String instead of null value.
                        if(confirmationMethodTimeString != null && !confirmationMethodTimeString.equals("null")) {
                            Date dateTime = sdf.parse(confirmationMethodTimeString);
                            currentInstant = dateTime.toInstant();
                        }
                        if (confirmationMethodTime == null || currentInstant.isAfter(confirmationMethodTime)) {
                            confirmationMethodTime = currentInstant;
                        }
                    confirmationMethodMap.put(node.get("confirmation_method_cd").asText(), node.get("confirmation_method_desc_txt").asText());
                }
            }
            investigationConfirmation.setInvestigationId(investigationId);

            if(confirmationMethodTime == null) {
                investigationConfirmation.setConfirmationMethodTime(phcLastChgTime);
            }
            for(String key : confirmationMethodMap.keySet()) {
                investigationConfirmation.setConfirmationMethodCd(key);
                investigationConfirmation.setConfirmationMethodDescTxt(confirmationMethodMap.get(key));
                kafkaTemplate.send(investigationConfirmationOutputTopicName, investigationConfirmation.toString());
            }
        } catch (Exception e) {
            logger.error("Error processing investigation confirmation method JSON array from investigation data: {}", e.getMessage());
        }
    }
}
