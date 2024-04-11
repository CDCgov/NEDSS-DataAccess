package gov.cdc.etldatapipeline.investigation.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import gov.cdc.etldatapipeline.investigation.repository.model.output.DataEnvelope;
import gov.cdc.etldatapipeline.investigation.repository.model.output.DataField;
import gov.cdc.etldatapipeline.investigation.repository.model.output.DataSchema;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

public class JsonGenerator {
    public DataEnvelope generateSchema(Object model) {

        Set<DataField> dataFields = new HashSet<>();
        try {
            for (Field field : model.getClass().getDeclaredFields()) {
                DataField dataField = new DataField();
                if (field.isAnnotationPresent(JsonProperty.class)) {
                    dataField.setField(field.getAnnotation(JsonProperty.class).value());
                } else {
                    dataField.setField(PropertyNamingStrategies.SnakeCaseStrategy.INSTANCE.translate(field.getName()));
                }
                dataField.setOptional(field.getName().equalsIgnoreCase("investigationUid"));
                dataField.setType(getType(field.getType().getSimpleName().toLowerCase()));
                dataFields.add(dataField);
            }
        } catch (Exception e) {
            throw e;
        }
        return new DataEnvelope(new DataSchema("struct", dataFields), model);
    }

    private String getType(String javaType) {
        return switch (javaType.toLowerCase()) {
            case "long" -> "int64";
            case "integer", "int" -> "int32";
            default -> javaType.toLowerCase();
        };
    }
}
