package org.datakow.core.components;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author kevin.off
 */
public class DotNotationListTest {
    
    public DotNotationListTest() {
    }

    @Test
    public void testFromJson() throws IOException {
        
        String json = "[{\"name\":\"kevin\"},{\"name\":\"bob\"}]";
        DotNotationList list = DotNotationList.fromJson(json);
        Assert.assertEquals(2, list.size());
        Assert.assertEquals(true, DotNotationMap.class.getName().equals(list.get(0).getClass().getName()));
        Assert.assertEquals(true, DotNotationMap.class.getName().equals(list.get(1).getClass().getName()));
        Assert.assertEquals("kevin", ((DotNotationMap)list.get(0)).getProperty("name"));
        Assert.assertEquals("bob", ((DotNotationMap)list.get(1)).getProperty("name"));
    }
    
}
