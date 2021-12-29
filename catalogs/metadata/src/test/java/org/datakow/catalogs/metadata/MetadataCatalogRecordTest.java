package org.datakow.catalogs.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DotNotationMap;
import java.util.Calendar;

import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kevin.off
 */
public class MetadataCatalogRecordTest {
    
    public MetadataCatalogRecordTest() {
    }

    @Test
    public void testToFromJson() throws JsonProcessingException {
        
        MetadataCatalogRecord orig = getMockRecord();
        String origJson = orig.toJson();
        MetadataCatalogRecord newRecord = MetadataCatalogRecord.fromJson(origJson);
        assertEquals(orig, newRecord);
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
