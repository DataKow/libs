package org.datakow.catalogs.metadata.jsonschema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

/**
 *
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonSchemaObject extends JsonSchemaProperty {

    private Integer maxProperties;
    private Integer minProperties;
    private List<String> required;
    private Object additionalProperties;
    private Map<String, JsonSchemaProperty> properties;
    private Map<String, Object> patternProperties;
    private Map<String, Object> dependencies;
    
    @Override
    public String getType() {
        return JsonSchemaType.object;
    }

    public Integer getMaxProperties() {
        return maxProperties;
    }

    public void setMaxProperties(Integer maxProperties) {
        this.maxProperties = maxProperties;
    }

    public Integer getMinProperties() {
        return minProperties;
    }

    public void setMinProperties(Integer minProperties) {
        this.minProperties = minProperties;
    }

    public List<String> getRequired() {
        return required;
    }

    public void setRequired(List<String> required) {
        this.required = required;
    }

    public Object getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Object additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public Map<String, JsonSchemaProperty> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, JsonSchemaProperty> properties) {
        this.properties = properties;
    }

    public Map<String, Object> getPatternProperties() {
        return patternProperties;
    }

    public void setPatternProperties(Map<String, Object> patternProperties) {
        this.patternProperties = patternProperties;
    }

    public Map<String, Object> getDependencies() {
        return dependencies;
    }

    public void setDependencies(Map<String, Object> dependencies) {
        this.dependencies = dependencies;
    }
    
    
    
}
