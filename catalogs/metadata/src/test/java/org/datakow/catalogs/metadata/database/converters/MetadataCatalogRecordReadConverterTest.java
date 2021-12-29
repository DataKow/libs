package org.datakow.catalogs.metadata.database.converters;

import com.mongodb.DBObject;

import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DotNotationMap;
import java.util.Calendar;

import org.bson.Document;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kevin.off
 */
public class MetadataCatalogRecordReadConverterTest {
    
    public MetadataCatalogRecordReadConverterTest() {
    }

    @Test
    public void testConversion() {
        
        MetadataCatalogRecordWriteConverter write = new MetadataCatalogRecordWriteConverter();
        Document writeResult = write.convert(getMockRecord());
        MetadataCatalogRecordReadConverter read = new MetadataCatalogRecordReadConverter();
        MetadataCatalogRecord record = read.convert(writeResult);
        assertEquals(getMockRecord(), record);
        
    }
    
    private MetadataCatalogRecord getMockRecord(){
        MetadataCatalogRecord record = new MetadataCatalogRecord();
        MetadataCatalogRecordStorage storage = new MetadataCatalogRecordStorage();
        DotNotationMap doc = new DotNotationMap();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2017, 3, 24, 12, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        storage.setId(mockMetadataCatalogIdentity().getRecordIdentifier());
        storage.setObjectIdentities(new CatalogIdentityCollection(mockObjectCatalogIdentity()));
        storage.setPublishDate(calendar.getTime());
        storage.setPublisher("datakow");
        storage.setRealm("public");
        
        doc.setProperty("property", "value");
        doc.setProperty("SomeDate", calendar.getTime());
        
        record.setStorage(storage);
        record.setDocument(doc);
        DotNotationMap metaDoc = new DotNotationMap();
        metaDoc.setProperty("distanceFromQueryPoint", 1.022112);
        return record;
    }
    
    private CatalogIdentity mockMetadataCatalogIdentity(){
        return new CatalogIdentity("DATAKOW_CATALOG", "b5a8616a-bffc-4ad54-c8a6-c873c4e618b3");
    }
    private CatalogIdentity mockObjectCatalogIdentity(){
        return new CatalogIdentity("DATAKOW_OBJECTS", "a5a8616a-bffc-4ad54-c8a6-c873c4e618b3");
    }
    
}
