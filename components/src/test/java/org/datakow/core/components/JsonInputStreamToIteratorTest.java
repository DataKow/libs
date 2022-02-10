package org.datakow.core.components;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author kevin.off
 */
public class JsonInputStreamToIteratorTest {
    
    public JsonInputStreamToIteratorTest() {
    }

    @Test
    public void testIterator() throws IOException {
        
        InputStream stream = new ByteArrayInputStream(getRecords().getBytes());
        
        JsonInputStreamToIterator<DotNotationMap> jisti = JsonInputStreamToIterator.makeIterator(stream, DotNotationMap.class);
        DotNotationList list = DotNotationList.fromJson(getRecords());
        int i = 0;
        
        while(jisti.hasNext()){
            Assert.assertEquals((String)((DotNotationMap)list.get(i)).getProperty("Record-Identifier"), (String)jisti.next().getProperty("Record-Identifier"));
            i++;
        }
        
    }
    
    @Test
    public void testMixedType() throws IOException{
        
        String arr = "[\"Kevin\",123,true,2.2]";
        InputStream stream = new ByteArrayInputStream(arr.getBytes());
        JsonInputStreamToIterator<Object> jisti = JsonInputStreamToIterator.makeIterator(stream, Object.class);
        Assert.assertEquals("Kevin", jisti.next());
        Assert.assertEquals(123, (int)jisti.next());
        Assert.assertEquals(true, (boolean)jisti.next());
        Assert.assertEquals(2.2D, (double)jisti.next(), 0);
    }
    
    private String getRecords(){
        return "[{\"Storage\":{\"Realm\":\"public\",\"Publisher\":\"datakow\",\"Publish-Date\":\"2017-02-13T17:31:55Z\",\"Object-Identities\":[{\"Catalog-Identifier\":\"DATAKOW_OBJECTS\",\"Record-Identifier\":\"61069619-8782-47c4-87c5-1daeba2239be\"}],\"Record-Identifier\":\"bf78a45f-0789-4144-9665-690ec6304710\"},\"Doc\":{\"Feed-Type\":\"WMO\",\"Wmo-Id\":\"SRUS31\",\"Issuing-Office\":\"KWOH\",\"Issue-Time\":\"2017-02-13T17:30:00Z\",\"Content-Type\":\"text/plain\",\"Product-Category\":\"TEXT-PRODUCT\",\"Product-Identifier\":\"RRS\",\"Location-Id\":\"PTR\"}},{\"Storage\":{\"Realm\":\"public\",\"Publisher\":\"datakow\",\"Publish-Date\":\"2017-02-13T17:31:55Z\",\"Object-Identities\":[{\"Catalog-Identifier\":\"DATAKOW_OBJECTS\",\"Record-Identifier\":\"ccb51f02-ea6f-41a2-80e2-9757e0435629\"}],\"Record-Identifier\":\"7a833304-3785-45aa-9038-3a702b71b45b\"},\"Doc\":{\"Feed-Type\":\"WMO\",\"Wmo-Id\":\"SRUS79\",\"Issuing-Office\":\"KWOH\",\"Issue-Time\":\"2017-02-13T17:30:00Z\",\"Content-Type\":\"text/plain\",\"Product-Category\":\"TEXT-PRODUCT\",\"Product-Identifier\":\"RRS\",\"Location-Id\":\"EPZ\"}}]";
    }
    
}
