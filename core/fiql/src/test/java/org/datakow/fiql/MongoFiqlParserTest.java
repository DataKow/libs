package org.datakow.fiql;

import org.bson.Document;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 *
 * @author kevin.off
 */
public class MongoFiqlParserTest {
    
    public MongoFiqlParserTest() {
    }

    @Test
    public void testParse_String() {
        
        MongoFiqlParser parser = new MongoFiqlParser();
        Criteria crit = parser.parse("prop==value");
        Assert.assertTrue(crit.getCriteriaObject().containsKey("prop"));
        Assert.assertTrue(crit.getCriteriaObject().get("prop").equals("value"));
        
        crit = parser.parse("prop==05");
        Assert.assertTrue(crit.getCriteriaObject().containsKey("prop"));
        Assert.assertTrue(crit.getCriteriaObject().get("prop").equals(5L));
        
        crit = parser.parse("prop==2.3");
        Assert.assertTrue(crit.getCriteriaObject().containsKey("prop"));
        Assert.assertTrue(crit.getCriteriaObject().get("prop").equals(2.3D));
        
        crit = parser.parse("prop==20070502000001Z");
        Calendar date = Calendar.getInstance(TimeZone.getTimeZone("CDT"));
        date.set(2007, 4, 2, 0, 0, 1);
        
        Assert.assertTrue(crit.getCriteriaObject().containsKey("prop"));
        Assert.assertTrue(crit.getCriteriaObject().get("prop").toString().equals(date.getTime().toString()));
        
    }
    @Test
    public void testLongNumber(){
        MongoFiqlParser parser = new MongoFiqlParser();
        Criteria crit = parser.parse("prop==1498668780000");
        Document obj = crit.getCriteriaObject();
        Assert.assertTrue(Long.class.isAssignableFrom(obj.get("prop").getClass()));
        Assert.assertEquals(1498668780000L, obj.get("prop"));
    }
    
}
