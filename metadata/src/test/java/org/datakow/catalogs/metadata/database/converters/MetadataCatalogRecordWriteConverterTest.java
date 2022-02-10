package org.datakow.catalogs.metadata.database.converters;

import org.datakow.core.components.DotNotationMap;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import org.bson.Document;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author kevin.off
 */
public class MetadataCatalogRecordWriteConverterTest {
    
    public MetadataCatalogRecordWriteConverterTest() {
    }

    @Test
    public void testConvert() {
        MetadataCatalogRecordWriteConverter converter = new MetadataCatalogRecordWriteConverter();
        MetadataCatalogRecord record = new MetadataCatalogRecord();
        MetadataCatalogRecordStorage storage = new MetadataCatalogRecordStorage();
        String id = UUID.randomUUID().toString();
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("UTC")));
        calendar.set(2017, 8, 8, 20, 06, 20);
        calendar.set(Calendar.MILLISECOND, 0);
        Date date = calendar.getTime();
        storage.setId(id);
        storage.setPublishDate(date);
        storage.setPublisher("publisher");
        storage.setRealm("realm");
        storage.setTags(Arrays.asList("one", "two"));
        storage.setUpdateDate(date);
        storage.setUpdatedBy("updated");
        record.setStorage(storage);
        record.setDocument(new DotNotationMap());
        Document document = converter.convert(record);
        Assert.assertEquals(((DotNotationMap)document.get("Storage")).getProperty("Publish-Date"), date);
    }
    
}
