package org.datakow.core.components;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Calendar;

/**
 * A custom Serializer used in Jackson2Json to parse Calendar objects using the 
 * {@link DateUtil} utility.
 * 
 * @author kevin.off
 */
public class CalendarSerializer extends JsonSerializer<Calendar>{

    /**
     * The serialize method used to serialize Calendar objects using {@link DateUtil}
     * 
     * @param t The calendar object to serialize
     * @param jg the generator
     * @param sp the provider
     * @throws IOException If there is an issue writing to the JSON parser
     * @throws JsonProcessingException If there was an issue with the json output
     */
    @Override
    public void serialize(Calendar t, JsonGenerator jg, SerializerProvider sp) throws IOException, JsonProcessingException {

        jg.writeString(DateUtil.calendarToString(t));

    }
        
}
