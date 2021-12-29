package org.datakow.catalogs.metadata.jsonschema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DatakowObjectMapper;
import java.util.Map;

/**
 *
 * @author kevin.off
 */
@JsonPropertyOrder({"$schema", "id", "title", "description", "type", "properties"})
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonSchema {

    private String schema;
    private String title;
    private String description;
    private String type;
    private String id;
    private Map<String, JsonSchemaProperty> properties;

    @JsonProperty("$schema")
    public String getSchema() {
        return schema;
    }

    @JsonProperty("$schema")
    public void setSchema(String schema) {
        this.schema = schema;
    }
    
    public void setId(String id){
        this.id = id;
    }
    
    public String getId(){
        return this.id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, JsonSchemaProperty> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, JsonSchemaProperty> properties) {
        this.properties = properties;
    }
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        return mapper.writeValueAsString(this);
    }
    
    public static JsonSchema fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        return mapper.readValue(json, JsonSchema.class);
    }
}
