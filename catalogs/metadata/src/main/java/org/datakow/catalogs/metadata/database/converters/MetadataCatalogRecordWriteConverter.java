package org.datakow.catalogs.metadata.database.converters;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.DatakowObjectMapper;

import org.bson.Document;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.springframework.core.convert.converter.Converter;

/**
 * Spring MongoDB converter that converts a {@link MetadataCatalogRecord} to a {@link DBObject}
 * when reading them from the database.
 * 
 * @author kevin.off
 */
public class MetadataCatalogRecordWriteConverter implements Converter<MetadataCatalogRecord, Document>{

    /**
     * Spring MongoDB converter that converts a {@link MetadataCatalogRecord} to a {@link Document}
     * when reading them from the database.
     * 
     * @param record The record to convert
     * @return The converted object
     */
    @Override
    public Document convert(MetadataCatalogRecord record) {
        Document recordDoc = new Document();
        
        if (record.getStorage() == null){
            throw new RuntimeException("The Storage property cannot be null");
        }
        recordDoc.append("_id", record.getStorage().getId());
        recordDoc.append("Storage", DatakowObjectMapper.getDateAwareObjectMapper().convertValue(record.getStorage(), DotNotationMap.class));
        recordDoc.append("Doc", record.getDocument());
        
        return new Document(recordDoc);
    }
    
}
