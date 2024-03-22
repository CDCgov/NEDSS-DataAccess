package gov.cdc.etldatapipeline.person.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.person.model.dto.DataEnvelope;
import gov.cdc.etldatapipeline.person.model.odse.DebeziumMetadata;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Objects;

@Slf4j
@NoArgsConstructor
public class UtilHelper {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static UtilHelper utilHelper;

    public static UtilHelper getInstance() {
        utilHelper = (utilHelper == null) ? new UtilHelper() : utilHelper;
        return utilHelper;
    }

    public <T> T parseJsonNode(String jsonString, String nodeName,
                               Class<T> type) {
        try {
            JsonNode node = objectMapper.readTree(jsonString).at(nodeName);
            return objectMapper.convertValue(node, type);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
            e.printStackTrace();
        }
        return null;
    }

    public <T extends DebeziumMetadata> T deserializePayload(
            String jsonString, String nodeName, Class<T> type) {
        try {
            JsonNode jsonTree = objectMapper.readTree(jsonString);
            JsonNode rootNode = jsonTree.at(nodeName);
            T dMetadata = objectMapper.convertValue(rootNode, type);
            if(!Objects.isNull(dMetadata)) {
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

    public <T> T deserializePayload(String jsonString, Class<T> type) {
        try {
            if (jsonString == null) return null;
            return objectMapper.readValue(jsonString, type);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
            e.printStackTrace();
        }
        return null;
    }

    public <T> DataEnvelope constructDataEnvelope(T obj) {
        JsonNode jsonNode;
        try {
            JsonSchema schema = JsonSchemaUtils.getSchema(obj);
            jsonNode = Objects.isNull(schema) ? null : schema.toJsonNode();
        } catch (IOException e) {
            //ToDo: Replace with Generic ExceptionHandler
            throw new RuntimeException(e);
        }
        return new DataEnvelope<>(jsonNode, obj);
    }
}
