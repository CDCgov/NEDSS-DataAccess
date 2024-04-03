package gov.cdc.etldatapipeline.organization.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.cdc.etldatapipeline.organization.model.DataRequiredFields;
import gov.cdc.etldatapipeline.organization.model.dto.dataprops.DataEnvelope;
import gov.cdc.etldatapipeline.organization.model.dto.dataprops.DataField;
import gov.cdc.etldatapipeline.organization.model.dto.dataprops.DataSchema;
import gov.cdc.etldatapipeline.organization.model.odse.DebeziumMetadata;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Slf4j
@NoArgsConstructor
public class UtilHelper {
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private static UtilHelper utilHelper;

    public static UtilHelper getInstance() {
        utilHelper = (utilHelper == null) ? new UtilHelper() : utilHelper;
        return utilHelper;
    }

    public <T extends DebeziumMetadata> T deserializePayload(
            String jsonString, String nodeName, Class<T> type) {
        try {
            JsonNode jsonTree = objectMapper.readTree(jsonString);
            JsonNode rootNode = jsonTree.at(nodeName);
            T dMetadata = objectMapper.convertValue(rootNode, type);
            if (!Objects.isNull(dMetadata)) {
                if (!Objects.isNull(jsonTree.at("/payload/ts_ms")))
                    dMetadata.setTs_ms(jsonTree.at("/payload/ts_ms").asLong());
                if (!Objects.isNull(jsonTree.at("/payload/op")))
                    dMetadata.setOp(jsonTree.at("/payload/op").asText());
            }
            return dMetadata;
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
            e.printStackTrace();
        }
        return null;
    }

    public <T extends DataRequiredFields> DataEnvelope constructDataEnvelope(T obj) {
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
            throw e;
        }
        return new DataEnvelope(new DataSchema("struct", dataFields), obj);
    }

    private String getType(String javaType) {
        return switch (javaType.toLowerCase()) {
            case "long" -> "int64";
            case "integer", "int" -> "int32";
            default -> javaType.toLowerCase();
        };
    }
}
