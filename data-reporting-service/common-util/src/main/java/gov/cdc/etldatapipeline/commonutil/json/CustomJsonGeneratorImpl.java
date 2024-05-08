package gov.cdc.etldatapipeline.commonutil.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.CaseFormat;
import gov.cdc.etldatapipeline.commonutil.model.DataRequiredFields;
import gov.cdc.etldatapipeline.commonutil.model.avro.DataEnvelope;
import gov.cdc.etldatapipeline.commonutil.model.avro.DataField;
import gov.cdc.etldatapipeline.commonutil.model.avro.DataSchema;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

public class CustomJsonGeneratorImpl {
    public <T extends DataRequiredFields> DataEnvelope buildAvroRecord(T obj) {
        Set<DataField> dataFields = new HashSet<>();
        try {
            for (Field field : obj.getClass().getDeclaredFields()) {
                DataField dataField = new DataField();
                if (field.isAnnotationPresent(JsonProperty.class)) {
                    dataField.setField(field.getAnnotation(JsonProperty.class).value());
                } else {
                    dataField.setField(PropertyNamingStrategies.SnakeCaseStrategy.INSTANCE.translate(field.getName()));
                }
                dataField.setOptional(
                        obj.getRequiredFields() == null || !obj.getRequiredFields().contains(field.getName()));
                dataField.setType(getType(field.getType().getSimpleName().toLowerCase()));
                dataFields.add(dataField);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error building schema record: ", e);
        }
        return new DataEnvelope(new DataSchema("struct", dataFields), obj);
    }

    public String generateStringJson(Object model) {
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

                String fieldName = field.getName();

                fieldName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName);

                fieldNode.put("type", getType(field.getType().getSimpleName().toLowerCase()));
                fieldNode.put("optional", !fieldName.equals("public_health_case_uid"));
                fieldNode.put("field", fieldName);
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
                String fieldName = field.getName();

                fieldName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName);

                payloadNode.put(fieldName, objectMapper.valueToTree(field.get(model)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return payloadNode;
    }

    private static String getType(String javaType) {
        return switch (javaType.toLowerCase()) {
            case "long" -> "int64";
            case "integer", "int" -> "int32";
            case "instant" -> "string";
            default -> javaType.toLowerCase();
        };
    }
}
