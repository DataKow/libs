package org.datakow.core.components;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Date;

/**
 * A custom Serializer used in Jackson2Json to parse Date objects using the 
 * {@link DateUtil} utility.
 * 
 * @author kevin.off
 */
public class DateSerializer extends JsonSerializer<Date>{

    /**
     * The serialize method used to serialize Date objects using {@link DateUtil}
     * 
     * @param t The Date object to serialize
     * @param jg the generator
     * @param sp the provider
     * @throws IOException If there is an issue writing to the JSON parser
     * @throws JsonProcessingException If there was an issue with the json output
     */
    @Override
    public void serialize(Date t, JsonGenerator jg, SerializerProvider sp) throws IOException, JsonProcessingException {

        jg.writeString(DateUtil.dateToUTCString(t));

    }

}
