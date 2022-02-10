package org.datakow.core.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kevin.off
 */
public class IteratorToInputStreamTest {
    
    public IteratorToInputStreamTest() {
    }

    @Test
    public void testIteratorToJsonArrayInputStream() throws IOException {
        
        IteratorToInputStream stream = IteratorToInputStream.iteratorToJsonArrayInputStream(Arrays.asList("one", "two", "three").iterator());
        String string = IOUtils.toString(stream);
        DotNotationList<String> list = new DotNotationList<>(Arrays.asList("one", "two", "three"));
        String listString = list.toJson();
        assertEquals(listString, string);
    }

    @Test
    public void testJsonObjectIteratorToJsonArrayInputStream() throws IOException {
        
        IteratorToInputStream stream = IteratorToInputStream.jsonObjectIteratorToJsonArrayInputStream(
                Arrays.asList(
                        "{\"prop\":123}", 
                        "{\"prop2\":\"stuff\"}", 
                        "{\"stuff\":[1,2,3]}").iterator());
        String string = IOUtils.toString(stream);
        DotNotationList<DotNotationMap> list = new DotNotationList<>(
                Arrays.asList(
                        DotNotationMap.fromJson("{\"prop\":123}"), 
                        DotNotationMap.fromJson("{\"prop2\":\"stuff\"}"), 
                        DotNotationMap.fromJson("{\"stuff\":[1,2,3]}")));
        String listString = list.toJson();
        assertEquals(listString, string);
        
    }

    @Test
    public void testJsonProducerIteratorToJsonArrayInputStream() throws IOException {
        DotNotationList<DotNotationMap> list = new DotNotationList<>(
                Arrays.asList(
                        DotNotationMap.fromJson("{\"prop\":123}"), 
                        DotNotationMap.fromJson("{\"prop2\":\"stuff\"}"), 
                        DotNotationMap.fromJson("{\"stuff\":[1,2,3]}")));
        IteratorToInputStream stream = IteratorToInputStream.jsonProducerIteratorToJsonArrayInputStream(list.iterator());
        String string = IOUtils.toString(stream);
        assertEquals(list.toJson(), string);
    }
    
}
