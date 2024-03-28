package gov.cdc.etldatapipeline.investigation.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.investigation.repository.InvestigationRepository;
import gov.cdc.etldatapipeline.investigation.repository.model.dto.DummyClass;
import gov.cdc.etldatapipeline.investigation.repository.model.dto.Investigation;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class DummyService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final InvestigationRepository investigationRepository;

    public void sendMessage(String topicName, String jsonData) {
        DummyClass dummy = new DummyClass(1004L, "Anne", "Kretchmar", "annek@noanswer.org");

        Optional<Investigation> investigationData = investigationRepository.computeInvestigations("263771897");

        String message = generateJson(dummy);

        Investigation investigation = investigationData.get();

        String investigationMessage = generateJson(investigation);

   //kafkaTemplate.send(topicName, message);
    System.err.println(message);
        System.err.println("...................");
        System.err.println(investigationMessage);
    }

    private String generateJson(Object model) {
        try {
            ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
            ObjectNode root = objectMapper.createObjectNode();
            ObjectNode schemaNode = root.putObject("schema");
            schemaNode.put("type", "struct");
            schemaNode.set("fields", generateFieldsArray(model));
            ObjectNode payloadNode = root.putObject("payload");
            payloadNode = generatePayloadNode(payloadNode, model);
            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static ArrayNode generateFieldsArray(Object model) {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        ArrayNode fieldsArray = objectMapper.createArrayNode();

        try {
            Class<?> modelClass = model.getClass();
            for (Field field : modelClass.getDeclaredFields()) {
                ObjectNode fieldNode = objectMapper.createObjectNode();
                fieldNode.put("type", getType(field.getType().getSimpleName().toLowerCase()));
                fieldNode.put("optional", false);
                fieldNode.put("field", field.getName());
                fieldsArray.add(fieldNode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return fieldsArray;
    }

    private static ObjectNode generatePayloadNode(ObjectNode payloadNode, Object model) {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

        try {
            Class<?> modelClass = model.getClass();
            for (java.lang.reflect.Field field : modelClass.getDeclaredFields()) {
                field.setAccessible(true);
                payloadNode.put(field.getName(), objectMapper.valueToTree(field.get(model)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return payloadNode;
    }

    private static String getType(String javaType) {
        switch (javaType.toLowerCase()) {
            case "long":
                return "int64";
            case "integer":
            case "int":
                return "int32";
            case "string":
                return "string";
            default:
                return null;
        }
    }
}
