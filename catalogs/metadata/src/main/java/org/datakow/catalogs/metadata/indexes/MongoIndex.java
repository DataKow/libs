package org.datakow.catalogs.metadata.indexes;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.DatakowObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a MongoDB index of any type.
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MongoIndex {

    private List<MongoIndexField> indexFields = new ArrayList<>();
    private String name;
    private boolean unique = false;

    /**
     * Creates an instance of MongoIndex with no values set.
     */
    public MongoIndex(){}
    
    /**
     * Creates a MongoIndex with a given name.
     * 
     * @param name The name of the index (this is not the property name)
     */
    public MongoIndex(String name){
        this.name = name;
    }
    
    /**
     * Creates a default type index on the key with the given name in ASC order.
     * @param name The name of the index
     * @param key The property name to index
     */
    public MongoIndex(String name, String key){
        this.name = name;
        MongoIndexField field = new MongoIndexField(key, "ASC");
        this.indexFields.add(field);
    }
    
    /**
     * Creates a default type index on the key with the given direction
     * @param name The name of the index
     * @param key The property name to index
     * @param direction The direction ASC or DESC
     */
    public MongoIndex(String name, String key, String direction){
        this.name = name;
        MongoIndexField field = new MongoIndexField(key, direction);
        this.indexFields.add(field);
    }
    
    /**
     * Adds a geometric index to the list of fields.
     * 
     * @param fieldName The name of the field that is geometric
     */
    public void addGeoField(String fieldName){
        MongoIndexField field = new MongoIndexField();
        field.setKey(fieldName);
        field.setIsGeo(true);
        this.addIndexField(field);
    }
    
    /**
     * Gets the list of fields in this index
     * 
     * @return The list of fields
     */
    public List<MongoIndexField> getIndexFields() {
        return indexFields;
    }

    /**
     * Sets the list of fields in the index
     * 
     * @param indexFields The list of fields to add to the index
     */
    public void setIndexFields(List<MongoIndexField> indexFields) {
        this.indexFields = indexFields;
    }

    /**
     * Adds an individual field index definition
     * 
     * @param field The field definition to index
     */
    public void addIndexField(MongoIndexField field){
        indexFields.add(field);
    }
    
    /**
     * Gets the name of the index. 
     * <p>
     * This is not the same as the name of the property
     * 
     * @return The name of the index
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the index.
     * <p>
     * This is not the same as the property name that the index is for.
     * 
     * @param name The name of the index
     */
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * Whether the index is a unique index of not.
     * 
     * @return true if the index is unique
     */
    public boolean getUnique(){
        return unique;
    }
    
    /**
     * Set whether the index is unique or not
     * 
     * @param unique true if the index should be unique
     */
    public void setUnique(boolean unique){
        this.unique = unique;
    }
    
    public String toJson() throws JsonProcessingException {
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static List<MongoIndex> fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        List<MongoIndex> returnList = new ArrayList<>();
        if (json.trim().startsWith("[")){
            DotNotationList list = DotNotationList.fromJson(json);
            for(Object obj : list){
                if (obj instanceof DotNotationMap){
                    returnList.add(mapper.readValue(((DotNotationMap)obj).toJson(), MongoIndex.class));
                }
            }
        }else{
            MongoIndex record = mapper.readValue(json, MongoIndex.class);
            returnList.add(record);
        }
        
        return returnList;

    }
    
    @Override
    public boolean equals(Object obj){
        if (obj == null) {
            return false;
        }
        if (!MongoIndex.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        if (obj == this){
            return true;
        }
        MongoIndex other = (MongoIndex)obj;
        
        boolean equals = (this.getIndexFields()== null ? other.getIndexFields() == null : this.getIndexFields().equals(other.getIndexFields()));
        equals = equals && (this.getName()== null ? other.getName() == null : this.getName().equals(other.getName()));
        equals = equals && this.getUnique() == other.getUnique();
        
        return equals;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + Objects.hashCode(this.indexFields);
        hash = 61 * hash + Objects.hashCode(this.name);
        hash = 61 * hash + (this.unique ? 1 : 0);
        return hash;
    }
    
}
