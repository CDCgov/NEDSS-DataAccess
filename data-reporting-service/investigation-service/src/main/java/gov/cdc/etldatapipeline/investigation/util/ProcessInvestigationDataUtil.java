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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        transformObservationNotificationIds(investigation.getObservationNotificationIds(), investigationTransformed, objectMapper);
        transforminvestigationConfirmationMethod(investigation.getInvestigationConfirmationMethod(), investigationTransformed, objectMapper);

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

    private void transformObservationNotificationIds(String observationNotificationIds, InvestigationTransformed investigationTransformed, ObjectMapper objectMapper) {
        try {
            JsonNode observationNotificationIdsJsonArray = observationNotificationIds != null ? objectMapper.readTree(observationNotificationIds) : null;
            InvestigationNotification investigationNotification = new InvestigationNotification();
            InvestigationObservation investigationObservation = new InvestigationObservation();
            List<Long> notificationIds = new ArrayList<>();
            List<Long> observationIds = new ArrayList<>();

            if(observationNotificationIdsJsonArray != null && observationNotificationIdsJsonArray.isArray()) {
                for(JsonNode node : observationNotificationIdsJsonArray) {
                    String sourceClassCode = node.get("source_class_cd").asText();
                    String actTypeCode = node.get("act_type_cd").asText();

                    if(sourceClassCode.equals("OBS") && actTypeCode.equals("PHCInvForm")) {
                        investigationTransformed.setPhcInvFormId(node.get("source_act_uid").asLong());
                    }
                    if(sourceClassCode.equals("NOTF") && actTypeCode.equals("Notification")) {
                        investigationNotification.setInvestigationId(node.get("public_health_case_uid").asLong());
                        notificationIds.add(node.get("source_act_uid").asLong());
                    }
                    if(sourceClassCode.equals("OBS") && actTypeCode.equals("LabReport")) {
                        investigationObservation.setInvestigationId(node.get("public_health_case_uid").asLong());
                        observationIds.add(node.get("source_act_uid").asLong());
                    }
                }
                investigationNotification.setNotificationId(notificationIds);
                investigationObservation.setObservationId(observationIds);
                kafkaTemplate.send(investigationObservationOutputTopicName, investigationObservation.toString());
                kafkaTemplate.send(investigationNotificationOutputTopicName, investigationObservation.toString());
            }
        } catch (Exception e) {
            logger.error("Error processing Observation Notification Ids JSON array from investigation data: {}", e.getMessage());
        }
    }

    private void transforminvestigationConfirmationMethod(String investigationConfirmationMethod, InvestigationTransformed investigationTransformed, ObjectMapper objectMapper) {
        try {
            JsonNode investigationConfirmationMethodJsonArray = investigationConfirmationMethod != null ? objectMapper.readTree(investigationConfirmationMethod) : null;
            InvestigationConfirmation investigationConfirmation = new InvestigationConfirmation();
            Long investigationId = null;
            Map<String, String> confirmationMethodMap = new HashMap<>();
            Instant confirmationMethodTime = null;
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

            // Redundant time variable in case if all confirmation_method_time is null
            String phcLastChgTime = null;


            if(investigationConfirmationMethodJsonArray != null && investigationConfirmationMethodJsonArray.isArray()) {
                investigationId = investigationConfirmationMethodJsonArray.get(0).get("public_health_case_uid").asLong();
                phcLastChgTime = investigationConfirmationMethodJsonArray.get(0).get("phc_last_chg_time").asText();
                for(JsonNode node : investigationConfirmationMethodJsonArray) {
                    String confirmationMethodTimeString = node.get("confirmation_method_time").asText().replaceAll("\"", "");
                    //LocalDateTime time = LocalDateTime.parse(confirmationMethodTimeString);;
                    Instant instant = null;
                    if(confirmationMethodTimeString != null && !confirmationMethodTimeString.equals("null")) {
                        LocalDateTime time = LocalDateTime.parse(confirmationMethodTimeString);
                        instant = time.toInstant(ZoneOffset.UTC);
                    }
                    if (confirmationMethodTime == null || instant.isAfter(confirmationMethodTime)) {
                        confirmationMethodTime = instant;
                    }
                    confirmationMethodMap.put(node.get("confirmation_method_cd").asText(), node.get("confirmation_method_desc_txt").asText());
                }
                System.err.println("confirmationMethodMap is..." + confirmationMethodMap);
            }
            investigationConfirmation.setInvestigationId(investigationId);
            if(confirmationMethodTime == null) {
                investigationConfirmation.setConfirmationMethodTime(phcLastChgTime);
            }
            for(String key : confirmationMethodMap.keySet()) {
                investigationConfirmation.setConfirmationMethodCd(key);
                investigationConfirmation.setConfirmationMethodDescTxt(confirmationMethodMap.get(key));
                System.err.println("Investigation id..." + investigationId);
                System.err.println("confirmationMethodTime is..." + confirmationMethodTime);
                System.err.println("phcLastChgTime is..." + phcLastChgTime);
                System.err.println("confirmationMethodMap is..." + confirmationMethodMap);
                kafkaTemplate.send(investigationConfirmationOutputTopicName, investigationConfirmation.toString());
            }

        } catch (Exception e) {
            logger.error("Error processing investigation confirmation method JSON array from investigation data: {}", e.getMessage());
        }
    }
}
