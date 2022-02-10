package org.datakow.catalogs.metadata.jsonpatch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.JsonProducer;
import org.datakow.core.components.DatakowObjectMapper;

/**
 *
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonPatchOperation implements JsonProducer{
    
    private JsonPatchOperationType operation;
    private String from;
    private String path;    
    private Object value;

    
    public JsonPatchOperation(){}
    
    public JsonPatchOperation(JsonPatchOperationType operation){
        this.operation = operation;
    }
    
    public static JsonPatchOperation add(String path, Object value){
        JsonPatchOperation op = new JsonPatchOperation(JsonPatchOperationType.add);
        op.setPath(path);
        op.setValue(value);
        return op;
    }
    
    public static JsonPatchOperation remove(String path){
        JsonPatchOperation op = new JsonPatchOperation(JsonPatchOperationType.remove);
        op.setPath(path);
        return op;
    }
    
    public static JsonPatchOperation replace(String path, Object value){
        JsonPatchOperation op = new JsonPatchOperation(JsonPatchOperationType.replace);
        op.setPath(path);
        op.setValue(value);
        return op;
    }
    
    public static JsonPatchOperation copy(String from, String path){
        JsonPatchOperation op = new JsonPatchOperation(JsonPatchOperationType.copy);
        op.setFrom(from);
        op.setPath(path);
        return op;
    }
    
    public static JsonPatchOperation move(String from, String path){
        JsonPatchOperation op = new JsonPatchOperation(JsonPatchOperationType.move);
        op.setFrom(from);
        op.setPath(path);
        return op;
    }
    
    public static JsonPatchOperation test(String path, Object value){
        JsonPatchOperation op = new JsonPatchOperation(JsonPatchOperationType.test);
        op.setPath(path);
        op.setValue(value);
        return op;
    }
    
    @JsonProperty("op")
    public JsonPatchOperationType getOperation() {
        return operation;
    }

    @JsonProperty("op")
    public void setOperation(JsonPatchOperationType operation) {
        this.operation = operation;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
    
    @Override
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        return mapper.writeValueAsString(this);
    }
    
}
