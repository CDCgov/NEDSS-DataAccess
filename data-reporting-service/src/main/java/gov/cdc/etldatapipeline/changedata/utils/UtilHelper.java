package gov.cdc.etldatapipeline.changedata.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.changedata.model.odse.DebeziumMetadata;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UtilHelper {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static UtilHelper utilHelper;

    private UtilHelper() {
    }

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
            JsonNode node = objectMapper.readTree(jsonString).at(nodeName);
            T dMetadata = objectMapper.convertValue(node, type);
            dMetadata.setTs_ms(objectMapper.readTree(jsonString).at("/payload/ts_ms").asLong());
            dMetadata.setOp(objectMapper.readTree(jsonString).at("/payload/op").asText());
            return dMetadata;
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
            e.printStackTrace();
        }
        return null;
    }

    public <T extends DebeziumMetadata> T deserializePayload(
            String jsonString, String nodeName, JavaType type) {
        try {
            if(jsonString == null || type == null) return null;
            JsonNode node = objectMapper.readTree(jsonString).at(nodeName);
            if(node == null) return null;
            T dMetadata = objectMapper.readValue(node.textValue(), type);
            dMetadata.setTs_ms(objectMapper.readTree(jsonString).at("/payload/ts_ms").asLong());
            dMetadata.setOp(objectMapper.readTree(jsonString).at("/payload/op").asText());
            return dMetadata;
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
            e.printStackTrace();
        }
        return null;
    }

    public <T> T deserializePayload(
            String jsonString, JavaType type) {
        try {
            if(jsonString == null) return null;
            return objectMapper.readValue(jsonString, type);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
            e.printStackTrace();
        }
        return null;
    }


}
