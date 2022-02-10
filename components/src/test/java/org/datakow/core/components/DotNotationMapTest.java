package org.datakow.core.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kevin.off
 */
public class DotNotationMapTest {
    
    public DotNotationMapTest() {
    }

    @Test
    public void testRemove() {
        
        DotNotationMap map = new DotNotationMap();
        map.setProperty("thing.stuff", "stuff");
        map.setProperty("thing.otherStuff", "otherStuff");
        map.remove("thing.otherStuff");
        map.setProperty("thing.thing.thing", "thingthingthing");
        map.remove("thing.thing.thing");
        assertNull(map.getProperty("thing.otherStuff"));
        assertNotNull(map.getProperty("thing.stuff"));
         
    }
    
    @Test
    public void testContainsKey(){
        
        DotNotationMap map = new DotNotationMap();
        map.setProperty("Property", "value");
        assertTrue(map.containsKey("Property"));
        map.setProperty("Sub.Prop", "value");
        assertTrue(map.containsKey("Sub.Prop"));
        map.setProperty("A.Very.Sub.Prop", "value");
        assertTrue(map.containsKey("A.Very.Sub.Prop"));
        
        map.setProperty("property", new ArrayList());
        map.setProperty("property.0.arrayproperty", "someValue");
        assertTrue(map.containsKey("property.0.arrayproperty"));
        
    }
    
    @Test
    public void testDateConversion() throws JsonProcessingException{
 
        DotNotationMap map = new DotNotationMap();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2017, 3, 24, 12, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date d = calendar.getTime();
        map.setProperty("SomeDate", d);
        
        String json = DatakowObjectMapper.getDatakowDateAwareObjectMapper().writeValueAsString(map);
        
        DotNotationMap newMap = DotNotationMap.fromJson(json);
        assertEquals(Date.class, newMap.getProperty("SomeDate").getClass());
        assertEquals((Date)map.getProperty("SomeDate"), (Date)newMap.getProperty("SomeDate"));
        
    }
    
    @Test
    public void testFlatten(){
        DotNotationMap map = new DotNotationMap();
        map.setProperty("prop", "val");
        map.setProperty("property.subProperty.subsubproperty", "subsub");
        map.setProperty("property.sub.sub.sub.sub", "bananas");
        
        Map<String, Object> flattened = map.flatten();
        assertEquals("val", flattened.get("prop"));
        assertEquals("subsub", flattened.get("property.subProperty.subsubproperty"));
        assertEquals("bananas", flattened.get("property.sub.sub.sub.sub"));
        
    }
    
    
    
}
