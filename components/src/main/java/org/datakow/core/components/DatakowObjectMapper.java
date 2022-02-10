package org.datakow.core.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

/**
 * Contains static instances to a regular Jackson2Json ObjectMapper and an ObjectMapper.
 * <p>
 * The date aware object mapper will automatically use the DateUtil serialize 
 * Date and Calendar objects using the specified format. 
 * Every library and application that converts JSON to Objects or Objects to JSON uses this class.
 * @author kevin.off
 */
public class DatakowObjectMapper extends ObjectMapper{
    
    private static DatakowObjectMapper objectMapper = null;
    private static DatakowObjectMapper dAObjectMapper = null;
    
    
    public static DatakowObjectMapper getDatakowObjectMapper(){
        if (objectMapper == null){
            objectMapper = new DatakowObjectMapper();
        }
        return objectMapper;
    }
    
    public static DatakowObjectMapper getDatakowDateAwareObjectMapper(){
        if (dAObjectMapper == null){
            dAObjectMapper = new DatakowObjectMapper();
            SimpleModule calendarModule = new SimpleModule();
            calendarModule.addSerializer(Calendar.class, new CalendarSerializer());
            dAObjectMapper.registerModule(calendarModule);

            SimpleModule dateModule = new SimpleModule();
            calendarModule.addSerializer(Date.class, new DateSerializer());
            dAObjectMapper.registerModule(dateModule);
        }
        return dAObjectMapper;
    }
    
    /**
     * Creates a regular instance of the object mapper to use.
     * 
     * @return the object mapper
     */
    public static ObjectMapper getObjectMapper(){
        
        if (objectMapper == null){
            objectMapper = new DatakowObjectMapper();
        }
        return objectMapper;
    }
    
    /**
     * Gets the date aware object mapper that will serialize dates correctly
     * 
     * @return the date aware object mapper
     */
    public static ObjectMapper getDateAwareObjectMapper(){
        
        if (dAObjectMapper == null){
        
            dAObjectMapper = new DatakowObjectMapper();

            SimpleModule calendarModule = new SimpleModule();
            calendarModule.addSerializer(Calendar.class, new CalendarSerializer());
            dAObjectMapper.registerModule(calendarModule);

            SimpleModule dateModule = new SimpleModule();
            calendarModule.addSerializer(Date.class, new DateSerializer());
            dAObjectMapper.registerModule(dateModule);
        }
        return dAObjectMapper;
    };
    
    
    
    @Override
    public <T> T readValue(String content, Class<T> valueType)
        throws JsonProcessingException{
        
        try {
            return super.readValue(content, valueType);
        } catch (IOException ex) {
            if (ex instanceof JsonProcessingException){
                throw (JsonProcessingException)ex;
            }else{
                throw new IllegalStateException(ex);
            }
        }
    }
    
    @Override
    public <T> T readValue(String content, TypeReference<T> valueTypeRef)
        throws JsonProcessingException{
        try {
            return super.readValue(content, valueTypeRef);
        } catch (IOException ex) {
            if (ex instanceof JsonProcessingException){
                throw (JsonProcessingException)ex;
            }else{
                throw new IllegalStateException(ex);
            }
        }
    }
    
    @Override
    public <T> T readValue(String content, JavaType valueType)
        throws JsonProcessingException{
        try {
            return super.readValue(content, valueType);
        } catch (IOException ex) {
            if (ex instanceof JsonProcessingException){
                throw (JsonProcessingException)ex;
            }else{
                throw new IllegalStateException(ex);
            }
        }
    }
    
}
