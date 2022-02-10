package org.datakow.catalogs.metadata.database.converters;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import org.datakow.core.components.DotNotationMap;
import java.util.Map;

import org.bson.Document;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;
import org.springframework.core.convert.converter.Converter;

/**
 * Spring MongoDB converter that converts a {@link DBObject} to a {@link MetadataCatalogRecord}
 * when writing them to the database.
 * @author kevin.off
 */
public class MetadataCatalogRecordReadConverter implements Converter<Document, MetadataCatalogRecord>{

    /**
     * Spring MongoDB converter that converts a {@link DBObject} to a {@link MetadataCatalogRecord}
     * when writing them to the database.
     * 
     * @param recordDBObject The DBObject to convert
     * @return The converted object
     */
    @Override
    @SuppressWarnings("unchecked")
    public MetadataCatalogRecord convert(Document recordDBObject) {
        if (recordDBObject == null){
            return null;
        }
        MetadataCatalogRecord record = new MetadataCatalogRecord();
        
        if (recordDBObject.containsKey("Storage")){
            record.setStorage(MetadataCatalogRecordStorage.fromMap((Map)recordDBObject.get("Storage")));
        }
        if (recordDBObject.containsKey("Doc")){
            record.setDocument(new DotNotationMap((Map)recordDBObject.get("Doc")));
        }
        return record;
    }

    
    
}
