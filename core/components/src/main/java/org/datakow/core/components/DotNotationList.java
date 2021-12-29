package org.datakow.core.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class can be used as a regular Linked List but may also be used
 * to access sub lists by using Dot Notation.
 * <p>
 * For example: int val = myList.getProperty("1.2.3")
 * 
 * @author kevin.off
 */
public class DotNotationList<T> extends LinkedList<T> implements JsonProducer{
    
    /**
     * Creates a new instance
     */
    public DotNotationList(){}
    
    /**
     * Creates a new instance by copying the contents of the given list
     * @param l The list to copy
     */
    public DotNotationList(List<T> l){
        if (l != null){
            for(int i = 0; i < l.size(); i++){
                this.setValueInternal(i, l.get(i));
            }
        }
    }
    
    /**
     * Gets the value from this and sub lists by using dot notation.
     * <p>
     * You may access a list within a list, within a list by calling
     * getProperty("2.5.5"). This would be the same as get(2).get(5).get(5)
     * 
     * @param <T> The return type to cast the value to
     * @param propertyPath The dot notation key to retrieve the data by
     * @return The data at that location or null
     */
    public <T> T getProperty(String propertyPath){
        //Example: myList.getProperty("1.thing.2.thing2");
        
        String[] path = propertyPath.split("\\.");
        String token = path[0];
        
        //if it is a number
        if (token.matches("^\\d+$")){
            //if the entire path is only 1 token then return the value
            Object val = super.get(Integer.parseInt(token));
            if (path.length == 1){
                return (T)val;
            }
            String nextPath = propertyPath.replaceFirst(token + "\\.", "");
            if (val instanceof DotNotationList){
                return (T)((DotNotationList)val).getProperty(nextPath);
            }else if (val instanceof DotNotationMap){
                return ((DotNotationMap)val).getProperty(nextPath);
            }else{
                return null;
            }
        }else{
            if (super.size() == 1 && super.get(0) instanceof DotNotationMap){
                return ((DotNotationMap)super.get(0)).getProperty(propertyPath);
            }
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, "You cannot traverse a list with a the token {0} because it is not an integer", token);
            return null;
        }
    }
   
    /**
     * Sets the value in this list at the given path
     * 
     * @param propertyPath The path to set the property
     * @param propertyValue The value to put there
     */
    public void setProperty(String propertyPath, T propertyValue){
        
        String[] path = propertyPath.split("\\.");
        String token = path[0];
        
        if (token.matches("^\\d+$")){
            int index = Integer.valueOf(token);
            if (path.length == 1){
                setValueInternal(index, propertyValue);
            }else{
                String nextToken = path[1];
                if (index > this.size() - 1 || super.get(index) == null){
                    if (nextToken.matches("^\\d+$")){
                        DotNotationList list = new DotNotationList();
                        list.setProperty(propertyPath.replaceFirst(token + "\\.", ""), propertyValue);
                        this.setValueInternal(index, (T)list);
                    }else{
                        DotNotationMap map = new DotNotationMap();
                        map.setProperty(propertyPath.replaceFirst(token + "\\.", ""), propertyValue);
                        this.setValueInternal(index, (T)map);
                    }
                }else{
                    Object val = super.get(index);
                    if (val instanceof DotNotationMap){
                        ((DotNotationMap)val).setProperty(propertyPath.replaceFirst(token + "\\.", ""), propertyValue);
                        this.setValueInternal(index, (T)val);
                    }else if(val instanceof DotNotationList){
                        ((DotNotationList)val).setProperty(propertyPath.replaceFirst(token + "\\.", ""), propertyValue);
                        this.setValueInternal(index, (T)val);
                    }else{
                        throw new RuntimeException("Cannot set sub properties after " + token + " if current value is not a DotNotationMap or a DotNotationList. Your value is a " + val.getClass().getName());
                    }
                }
            }
            
        }else{
            throw new RuntimeException("The first token of the key must be a string representation of an integer. " + token + " will not work as an index for a list");
        }
        
    }
    
    /**
     * The internal mechanism of setting values of this list.
     * <p>
     * This method will check if the object that you are setting the value to
     * is an object or a list and will act accordingly. 
     * <p>
     * Custom Mongo Functionality...
     * It will also check
     * for a special date type object that MongoDB uses and convert it to a
     * proper date object. 
     * <p>
     * Note, this method will not create a new entry in a list if you try to put
     * a value in an index that does not exist. i.e. key >= this.size()
     * 
     * @param key The index to put the value
     * @param value The valut to put there
     */
    private void setValueInternal(int key, T value){
        Object valueToAdd;
        if (value instanceof Map){
            if (((Map)value).keySet().size() == 1 && ((Map)value).containsKey("$date")){
                valueToAdd = ((Map)value).get("$date");
            }else{
                if (value instanceof DotNotationMap){
                    valueToAdd = value;
                }else{
                    valueToAdd = new DotNotationMap((Map)value);
                }
            }
        }else if (value instanceof List){
            if (value instanceof DotNotationList){
                valueToAdd = value;
            }else{
                valueToAdd = new DotNotationList((List)value);
            }
        }else{
            valueToAdd = convertValue(value);
        }
        if (this.isEmpty() || key == this.size()){
            super.add((T)valueToAdd);
        }else if(key < this.size()){
            super.set(key, (T)valueToAdd);
        }else{
            throw new IndexOutOfBoundsException("This DotNotationList only has " + this.size() + " elements. You cannot add a value at the " + key + " index.");
        }
    }
 
    public boolean containsKey(Object k){
        String key = (String)k;
        
        if (key.contains(".")){
            String[] path = key.split("\\.");
            String token = path[0];
            if (token.matches("^\\d+$")){
                int index = Integer.valueOf(token);
                if (index < this.size()){
                    Object val = super.get(index);
                    if (val instanceof DotNotationMap){
                        return ((DotNotationMap)val).containsKey(key.replaceFirst(token + "\\.", ""));
                    }else if (val instanceof DotNotationList){
                        return ((DotNotationList)val).containsKey(key.replaceFirst(token + "\\.", ""));
                    }else{
                        return false;
                    }
                }else{
                    return false;
                }
            }else{
                throw new IllegalArgumentException("You cannot access a list with the tocken " + token + ". It must be an integer");
            }
        }else{
            int index = Integer.valueOf(key);
            return index < this.size();
        }
    }
    
    /**
     * Check to see if the value is a string and that it can be parsed by the
     * {@link DateUtil}, it it is then it parses the string into a Date object.
     * 
     * @param value The object to convert
     * @return The converted value
     */
    public Object convertValue(Object value){
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
    public final boolean add(T e) {
        this.setProperty(String.valueOf(this.size()), e);
        return true;
    }

    @Override
    public final boolean addAll(Collection c) {
        int i = this.size();
        for(Object o : c){
            this.setProperty(String.valueOf(i), (T)o);
            i++;
        }
        return true;
    }

    @Override
    public final boolean addAll(int index, Collection c) {
        int i = index;
        for(Object o : c){
            this.setProperty(String.valueOf(i), (T)o);
            i++;
        }
        return true;
    }

    @Override
    public final T get(int index) {
        return this.getProperty(String.valueOf(index));
    }
    
    @Override
    public final T set(int index, T element) {
        T orig = null;
        if (index < this.size()){
            orig = super.get(index);
        }
        this.setProperty(String.valueOf(index), element);
        return orig;
    }

    @Override
    public final void add(int index, T element) {
        this.setProperty(String.valueOf(index), element);
    }
    
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowDateAwareObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static DotNotationList fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        DotNotationList identity = mapper.readValue(json, DotNotationList.class);
        return identity;
    }
    
}
