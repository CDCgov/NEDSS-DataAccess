package gov.cdc.etldatapipeline.changedata.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.cdc.etldatapipeline.changedata.model.odse.DebeziumMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.util.Assert;

@Slf4j
public class UtilHelper {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final UtilHelper utilHelper = null;

    private UtilHelper() {
    }

    public static UtilHelper getInstance() {
        return utilHelper == null ? new UtilHelper() : utilHelper;
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
            if(jsonString == null) return null;
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
            T dMetadata = objectMapper.readValue(jsonString, type);
            return dMetadata;
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException: ", e);
            e.printStackTrace();
        }
        return null;
    }


    public static GenericData.Record mapObjectToRecord(Object object) {
        if (object == null) return null;
        final Schema schema =
                ReflectData.get().getSchema(object.getClass());
        final GenericData.Record record = new GenericData.Record(schema);
        schema.getFields().forEach(r -> record.put(r.name(),
                PropertyAccessorFactory
                        .forDirectFieldAccess(object)
                        .getPropertyValue(r.name())));
        return record;
    }

    public static <T> T mapRecordToObject(GenericData.Record record,
                                          T object) {
        Assert.notNull(record, "record must not be null");
        Assert.notNull(object, "object must not be null");
        final Schema schema =
                ReflectData.get().getSchema(object.getClass());
        Assert.isTrue(schema.getFields().equals(
                        record.getSchema().getFields()),
                "Schema fields didn't match");
        record.getSchema().getFields().forEach(d
                -> PropertyAccessorFactory
                .forDirectFieldAccess(object)
                .setPropertyValue(d.name(), record.get(d.name()) == null
                        ? record.get(d.name()) :
                        record.get(d.name()).toString()));
        return object;
    }
}
