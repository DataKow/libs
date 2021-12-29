package org.datakow.catalogs.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.DatakowObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import org.apache.commons.io.IOUtils;

/**
 * The Doc section of a MetadataCatalogRecord object.
 * 
 * @author kevin.off
 */
public class MetadataCatalogRecordDocument extends DotNotationMap {
   
    /**
     * Creates an instance with no properties set
     */
    public MetadataCatalogRecordDocument(){}
    
    /**
     * Creates an instance by making a copy of the DotNotationMap
     * 
     * @param map The map to copy
     */
    public MetadataCatalogRecordDocument(DotNotationMap map){
        super(map);
    }
    
    /**
     * Creates an instance by making a copy of a Map
     * 
     * @param source The map to make a copy of
     */
    public MetadataCatalogRecordDocument(Map<String, Object> source){
        super(source);
    }
    
    @Override
    public String toJson() throws JsonProcessingException {
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowDateAwareObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    /**
     * Converts a JSON string into a MetadataCatalogRecordDocument.
     * @param json The JSON String to convert
     * @return The Converted document
     * @throws JsonProcessingException If there is a problem while parsing
     */
    public static MetadataCatalogRecordDocument fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        MetadataCatalogRecordDocument document = mapper.readValue(json, MetadataCatalogRecordDocument.class);
        return document;
    }
    
    /**
     * Writes the JSON String representation of this object to an output stream.
     * 
     * @param out The output stream to write to
     * @throws IOException When reading the JSON string fails  
     */
    public void writeTo(OutputStream out) throws IOException{
        IOUtils.copy(IOUtils.toInputStream(this.toJson()), out);        
    }
    
}
