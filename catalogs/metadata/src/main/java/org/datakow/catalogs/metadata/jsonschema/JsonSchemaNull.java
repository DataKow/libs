package org.datakow.catalogs.metadata.jsonschema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 *
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonSchemaNull extends JsonSchemaProperty{

    @Override
    public String getType() {
        return JsonSchemaType._null;
    }
    
}
