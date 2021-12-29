package org.datakow.core.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * It acts just like a regular LinkedHashMap except that you may access properties within it using dot notation.
 * <p>
 * For example, if you have a MongoDB object that has a property "Doc.Property.SubProperty" 
 * then you can just call dotNotationMap.getProperty("Doc.Property.SubProperty") and the 
 * method will recursively descend and retrieve the property. The DotNotationMap.setProperty() 
 * method will also create properties at any depth using dot notation.
 * @author kevin.off
 */
public class DotNotationMap extends LinkedHashMap<String, Object> implements JsonProducer {
     
    /**
     * Creates an instance
     */
    public DotNotationMap(){}
    
    /**
     * Creates an instance by copying the values from the given map
     * 
     * @param vals The map to copy
     */
    public DotNotationMap(Map<String, ?> vals){
        this.putAll(vals);
    }
    
    /**
     * Creates an instance by copying the values from the given DotNotaionMap
     * 
     * @param map The map to copy
     */
    public DotNotationMap(DotNotationMap map){
        super.putAll(map);
    }
    
    /**
     * Gets a property from the map by a given dot notation string.
     * 
     * @param <T> the assumed return type
     * @param propertyPath The dot notation string
     * @return The value or null
     */
    public <T> T getProperty(String propertyPath){
        String[] path = propertyPath.split("\\.");
        
        String token = path[0];
        //If the base token exists
        if (this.containsKey(token)){
            //Get the value of the base token
            Object val = super.get(token);
            //if the entire path is only 1 token then return the value
            if (path.length == 1){
                return (T)val;            
            }else if (val instanceof DotNotationMap){
                //if the value is an instance of a DotNotationMap then get the property from it
                return ((DotNotationMap)val).getProperty(propertyPath.replaceFirst(token + "\\.", "")); 
            }else if(val instanceof DotNotationList){
                //If the value is a list then we can create a new Dot Notation List with the remaining path
                return (T)((DotNotationList)val).getProperty(propertyPath.replaceFirst(token + "\\.", ""));
            }else{
                //if the value was not a list or a map and there are more tokens to parse then this is the end of the road
                return null;
            }
        }else{
            return null;
        }
    }
    
    /**
     * Sets a property in the map using a dot notation string
     * 
     * @param propertyPath The dot notation string
     * @param propertyValue The value
     */
    public final void setProperty(String propertyPath, Object propertyValue){
        String[] path = propertyPath.split("\\.");
        String token = path[0];
        
        if (path.length == 1){
            //If we are at the end of the path then we can got ahead and set the property value
            this.setValueInternal(token, propertyValue);
        }else{
            if (!this.containsKey(token)){
                DotNotationMap map = new DotNotationMap();
                map.setProperty(propertyPath.replaceFirst(token + "\\.", ""), propertyValue);
                this.setValueInternal(token, map);
            }else{
                Object val = this.getProperty(token);
                if (val instanceof DotNotationMap){
                    ((DotNotationMap)val).setProperty(propertyPath.replaceFirst(token + "\\.", ""), propertyValue);
                    this.setValueInternal(token, val);
                }else if(val instanceof DotNotationList){
                    ((DotNotationList)val).setProperty(propertyPath.replaceFirst(token + "\\.", ""), propertyValue);
                    this.setValueInternal(token, val);
                }else{
                    throw new RuntimeException("Cannot set sub properties after " + token + " if current value is not a DotNotationMap or a DotNotationList. Your value is a " + val.getClass().getName());
                }
            }
        }
    }
    
    /**
     * The internal mechanism of setting values of this map.
     * <p>
     * This method will check if the object that you are setting the value to
     * is an object or a list and will act accordingly. 
     * <p>
     * Custom Mongo Functionality...
     * It will also check
     * for a special date type object that MongoDB uses and convert it to a
     * proper date object. 
     * 
     * @param key The index to put the value
     * @param value The valut to put there
     */
    private void setValueInternal(String key, Object value){
        if (value instanceof Map){
            if (((Map)value).keySet().size() == 1 && ((Map)value).containsKey("$date")){
                setValueInternal(key, ((Map)value).get("$date"));
            }else{
                if (value instanceof DotNotationMap){
                    super.put(key, value);
                }else{
                    super.put(key, new DotNotationMap((Map)value));
                }
            }
        }else if (value instanceof List){
            if (value instanceof DotNotationList){
                super.put(key, value);
            }else{
                super.put(key, new DotNotationList((List)value));
            }
        }else{
            super.put(key, convertValue(key, value));
        }
    }
    
    /**
     * Check to see if the value is a string and that it can be parsed by the
     * {@link DateUtil}, it it is then it parses the string into a Date object.
     * 
     * @param key The property name
     * @param value The object to convert
     * @return The converted value
     */
    public Object convertValue(String key, Object value){
       
        if (value instanceof String){
            Date date = DateUtil.parseString((String)value);
            if (date != null){
                return date;
            }else{
                return value;
            }
        }
        return value;
    }
    
    @Override
    public final Object get(Object key) {
        if (key instanceof String){
            return this.getProperty((String)key);
        }else{
            throw new RuntimeException("The key must be a String and cannot be a " + key.getClass().getName());
        }
        
    }

    @Override
    public final Object put(String key, Object value) {
        Object orig = this.get(key);
        this.setProperty(key, value);
        return orig;
    }

    @Override
    public final void putAll(Map<? extends String, ? extends Object> m) {
        m.entrySet().stream().forEach((entry) -> this.setProperty(entry.getKey(), entry.getValue()));
    }
    
    @Override
    public boolean containsKey(Object k) {
        String key = (String)k;
        if (key.contains(".")){
            String[] path = key.split("\\.");
            String token = path[0];
            if (super.containsKey(token)){
                Object val = super.get(token);
                if (val instanceof DotNotationMap){
                    return ((DotNotationMap)val).containsKey(key.replaceFirst(token + "\\.", ""));
                }else if (val instanceof DotNotationList){
                    return ((DotNotationList)val).containsKey(key.replaceFirst(token + "\\.", ""));
                }else{
                    throw new IllegalStateException("The next value in the chain after " + token + " should be a DotNotationMap or a DotNotationList");
                }
            }else{
                return false;
            }
        }else{
            return super.containsKey(k);
        }
    }
    
    @Override
    public String toJson() throws JsonProcessingException {
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowDateAwareObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json; 
    }
    
    public static DotNotationMap fromJson(String json) throws JsonProcessingException {
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        DotNotationMap dotNotationMap = mapper.readValue(json, DotNotationMap.class);
        return dotNotationMap;
        
    }
    
    @Override
    public Object remove(Object key){
        if (((String)key).contains(".")){
            List<String> parts = Arrays.asList(((String)key).split("\\."));
            DotNotationMap root = this.getProperty(String.join(".", parts.subList(0, parts.size()-1)));
            return root.remove(parts.get(parts.size()-1));
        }else{
            return super.remove(key);
        }
    }
    
    public Map<String, Object> flatten(){
        Map<String, Object> flattened = new HashMap<>();
        for(String key : this.keySet()){
            flattened.putAll(flatten(key));
        }
        return flattened;
    }
    
    private Map<String, Object> flatten(String key){
        Map<String, Object> flattened = new HashMap<>();
        Object value = this.getProperty(key);
        if (value instanceof DotNotationMap){
            for(String subKey : ((DotNotationMap)value).keySet()){
                flattened.putAll(this.flatten(key + "." + subKey));
            }
        }else{
            flattened.put(key, value);
        }
        return flattened;
    }
    
    /**
     * Converts an object, using Jackson2 ObjectMapper to a DotNotationMap object.
     * If you are using this to create an object to store in MongoDb, only use it
     * if the object is not supported by Mongo. Example: Don't use it for a java.util.Date object.
     * @param obj The object to convert
     * @return The created DotNotationMap object
     * @throws com.fasterxml.jackson.core.JsonProcessingException On a parse exception
     */
    public static DotNotationMap fromObject(Object obj) throws JsonProcessingException{
        
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(obj);
        if (json != null){
            return DotNotationMap.fromJson(json);
        }
        return null;
    }
    
}
