package org.datakow.catalogs.metadata.jsonschema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DatakowObjectMapper;
import java.util.List;
import java.util.Map;

/**
 *
 * @author kevin.off
 */

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({ "type", "description", "index", "full-text" })
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(value=JsonSchemaArray.class, name="array"),
    @Type(value=JsonSchemaBoolean.class, name="boolean"),
    @Type(value=JsonSchemaInteger.class, name="integer"),
    @Type(value=JsonSchemaNull.class, name="null"),
    @Type(value=JsonSchemaNumber.class, name="number"),
    @Type(value=JsonSchemaObject.class, name="object"),
    @Type(value=JsonSchemaString.class, name="string")
})
public abstract class JsonSchemaProperty {
    
    String name;
    String description;
    Map<String, Object> customProperties;
    List<Object> enumeration;
    Boolean index;
    Boolean fullText;

    @JsonIgnore
    public String getName() {
        return name;
    }

    @JsonIgnore
    public void setName(String name) {
        this.name = name;
    }

    @JsonIgnore
    public abstract String getType();

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("enum")
    public List<Object> getEnum() {
        return enumeration;
    }

    @JsonProperty("enum")
    public void setEnum(List<Object> enumeration) {
        this.enumeration = enumeration;
    }
  
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        return mapper.writeValueAsString(this);
    }
}
