package org.datakow.catalogs.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.JsonProducer;
import org.datakow.core.components.DatakowObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import org.apache.commons.io.IOUtils;

/**
 * Class that represents the records that are stored in the Metadata Catalog Web Service.
 * 
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataCatalogRecord implements JsonProducer{
    
    private MetadataCatalogRecordStorage storage;
    private DotNotationMap doc = new DotNotationMap();
    private DotNotationMap metaDoc = null;
    private String correlationId;
   
    /**
     * Gets the Record-Identifier property from the {@link MetadataCatalogRecordStorage} object.
     * 
     * @return The record ID from the storage object
     */
    @JsonIgnore
    public String getId(){
        return getStorage() != null ? getStorage().getId() : null;
    }
    
    /**
     * Gets the storage portion of the record.
     * 
     * @return The storage object
     */
    @JsonProperty("Storage")
    public MetadataCatalogRecordStorage getStorage() {
        return storage;
    }

    /**
     * Sets the Storage portion of the record.
     * 
     * @param storage The storage object
     */
    @JsonProperty("Storage")
    public void setStorage(MetadataCatalogRecordStorage storage) {
        this.storage = storage;
    }
    
    /**
     * Sets the Doc portion of the record
     * 
     * @param document The doc section
     */
    @JsonProperty("Doc")
    public void setDocument(DotNotationMap document){
        this.doc = document;
    }
    
    /**
     * Gets the Doc portion of the record
     * 
     * @return The doc section
     */
    @JsonProperty("Doc")
    public DotNotationMap getDocument(){
        return this.doc;
    }
    
    /**
     * Gets the Correlation ID used in bulk operations
     * 
     * @return the correlationId
     */
    @JsonProperty("Correlation-Id")
    public String getCorrelationid(){
        return this.correlationId;
    }
    
    /**
     * Sets the Correlation ID used in bulk operations
     * 
     * @param correlationId the correlationId
     */
    @JsonProperty("Correlation-Id")
    public void setCorrelationId(String correlationId){
        this.correlationId = correlationId;
    }
    
    /**
     * Writes the JSON string representation of this object to an output stream.
     * 
     * @param out The output stream to write to
     * @throws JsonProcessingException When reading the JSON string fails  
     * @throws IOException if there is an error writing to the output stream
     */
    public void writeTo(OutputStream out) throws JsonProcessingException, IOException{
        IOUtils.copy(IOUtils.toInputStream(this.toJson()), out);
    }
    
    @Override
    public String toJson() throws JsonProcessingException {
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowDateAwareObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static MetadataCatalogRecord fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        MetadataCatalogRecord record = mapper.readValue(json, MetadataCatalogRecord.class);
        return record;
    }
    
    /**
     * Converts a JSON array of records to a list of record objects
     * 
     * @param jsonArray The JSON string
     * @return The converted list of record objects
     * @throws JsonProcessingException When reading JSON string fails
     */
    public static List<MetadataCatalogRecord> fromJsonArray(String jsonArray) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        List<MetadataCatalogRecord> records = mapper.readValue(jsonArray, mapper.getTypeFactory().constructCollectionType(List.class, MetadataCatalogRecord.class));
        return records;
        
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode(this.storage);
        hash = 29 * hash + Objects.hashCode(this.doc);
        hash = 29 * hash + Objects.hashCode(this.metaDoc);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MetadataCatalogRecord other = (MetadataCatalogRecord) obj;
        if (!Objects.equals(this.storage, other.storage)) {
            return false;
        }
        if (!Objects.equals(this.doc, other.doc)) {
            return false;
        }
        if (!Objects.equals(this.metaDoc, other.metaDoc)) {
            return false;
        }
        return true;
    }
    
}
