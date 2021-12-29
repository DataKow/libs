package org.datakow.catalogs.metadata.webservice;

import org.datakow.catalogs.metadata.database.MetadataDataCoherence;
import java.util.Arrays;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kevin.off
 */
public class MetadataCatalogWebserviceRequestTest {
    
    public MetadataCatalogWebserviceRequestTest() {
    }

    @Test
    public void testBuilder() {
        
        MetadataCatalogWebserviceRequest request = new MetadataCatalogWebserviceRequest()
                .withQuery("Doc.prop1==things")
                .withNear(-89.352, 43.24, 1000)
                .withGroupSort(Arrays.asList("Doc.thingie ASC","Doc.Other DESC"))
                .withGroupFunctions("min(Doc.Elevation),max(Doc.WindSpeed)")
                .withLimit(10)
                .withSort("Doc.someProp")
                .withGroupBy("Doc.property")
                .withDataCoherence(MetadataDataCoherence.CONSISTENT)
                .withProjectionProperties("Doc.prop1,Doc.prop2");
        String theString = request.toUrl("http://datakow.com/catalogs/DATAKOW_CATALOG/records");
        
        
        Map<String, String> params = request.getQueryParams();
        assertEquals(9, request.rawParams.size());
        assertEquals(9, params.size());
        for(Map.Entry<String, String> param : params.entrySet()){
            if (!theString.contains(param.getKey() + "=")){
                fail("Missing " + param.getKey());
            }
        }
        
    }
    
    @Test
    public void testBuilder2() {
        
        MetadataCatalogWebserviceRequest request = new MetadataCatalogWebserviceRequest()
                .withDataCoherence(MetadataDataCoherence.CONSISTENT)
                .withGroupBy("Doc.property")
                .withGroupFunctions("min(Doc.Elevation),max(Doc.WindSpeed)")
                .withLimit(10)
                .withProjectionProperties("Doc.prop1,Doc.prop2")
                .withQuery("Doc.prop1==things")
                .withSort("Doc.someProp");
        String theString = request.toUrl("http://datakow.com/catalogs/DATAKOW_CATALOG/records");
        
        
        
        Map<String, String> params = request.getQueryParams();
        assertEquals(7, request.rawParams.size());
        assertEquals(7, params.size());
        for(Map.Entry<String, String> param : params.entrySet()){
            if (!theString.contains(param.getKey() + "=")){
                fail("Missing " + param.getKey());
            }
        }
        
    }
    
}
