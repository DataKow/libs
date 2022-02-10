package org.datakow.fiql;

import org.datakow.core.components.DotNotationMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 *
 * @author kevin.off
 */
public class SubscriptionCriteriaTest {
    
    public SubscriptionCriteriaTest() {
    }

    @Test
    public void testComplexMatches() {
        
        SubscriptionCriteria sub;
        SubscriptionFiqlParser parser = new SubscriptionFiqlParser();
        
        sub = parser.parse("Doc.alert.info.area.geocode=matches=\"valueName==UGC;value=like='^(LC|LE|LH|LO|LM|LS|SL).*'\"");
        
        DotNotationMap doc = new DotNotationMap();
        doc.setProperty("Doc.alert.info.area.geocode", new ArrayList());
        
        doc.setProperty("Doc.alert.info.area.geocode.0.valueName", "UGC");
        doc.setProperty("Doc.alert.info.area.geocode.0.value", "MyBanana");
        
        doc.setProperty("Doc.alert.info.area.geocode.1.valueName", "UGC");
        doc.setProperty("Doc.alert.info.area.geocode.1.value", "LEBanana");
        
        doc.setProperty("Doc.alert.info.area.geocode.2.valueName", "STUFF");
        doc.setProperty("Doc.alert.info.area.geocode.2.value", "MyBanana");
        
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("Doc.alert.info.area.geocode=matches=\"valueName==STUFF;value=like='^(LC|LE|LH|LO|LM|LS|SL).*'\"");
        Assert.assertEquals(false, sub.meetsCriteria(doc));
    }
    
    @Test
    public void testSomeMethod() {
        
        
        DotNotationMap doc = new DotNotationMap();
        doc.setProperty("StringProperty", "kevin");
        doc.setProperty("IntegerProperty", 5);
        doc.setProperty("DoubleProperty", 5.5);
        // 1-1-2016 00:00:00 UTC
        doc.setProperty("DateProperty", new Date(1451606400));
        doc.setProperty("BooleanProperty", true);
        List<String> strings = new ArrayList<>();
        strings.add("kevin");
        strings.add("Bob");
        doc.setProperty("list", strings);
        doc.setProperty("Sub.Property", "subprop");

        SubscriptionFiqlParser parser = new SubscriptionFiqlParser();
        
        SubscriptionCriteria sub;
        
        sub = parser.parse("StringProperty==kevin;IntegerProperty==5;DoubleProperty=gt=4");
        
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("StringProperty==kevin;IntegerProperty==5;DoubleProperty=gt=4;NonExistantProperty!=5");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("StringProperty==kevin;IntegerProperty==5;DoubleProperty=gt=4;NonExistantProperty==5");
        Assert.assertEquals(false, sub.meetsCriteria(doc));
        
        sub = parser.parse("StringProperty==kevin;IntegerProperty==5;DoubleProperty=gt=4;StringProperty=in=(kevin,bob)");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("StringProperty==kevin;IntegerProperty==5;DoubleProperty=gt=4;StringProperty=out=(kevin,bob)");
        Assert.assertEquals(false, sub.meetsCriteria(doc));
        
        sub = parser.parse("StringProperty==kevin;IntegerProperty==5;DoubleProperty=gt=4;NonExistantProperty=out=(kevin,bob)");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("IntegerProperty=gt=4");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("IntegerProperty=ge=5");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("IntegerProperty=lt=6");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("IntegerProperty=le=75");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("IntegerProperty!=6");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("list=all=(kevin,Bob)");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("StringProperty=like=^kev.*n");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("StringProperty=like=^bev.*n");
        Assert.assertEquals(false, sub.meetsCriteria(doc));
         
        sub = parser.parse("StringProperty=in=(stuff,kevin,things)");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("IntegerProperty=in=(2,3,4,8,9,7,12)");
        Assert.assertEquals(false, sub.meetsCriteria(doc));
        
        sub = parser.parse("IntegerProperty=out=(2,3,4,8,9,7,12)");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("NoProperty!=stuff");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("NoProperty==stuff");
        Assert.assertEquals(false, sub.meetsCriteria(doc));
        
        sub = parser.parse("NoProperty=out=(things,stuff)");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("NoProperty=in=(things,stuff)");
        Assert.assertEquals(false, sub.meetsCriteria(doc));
        
        sub = parser.parse("NoProperty=exists=true");
        Assert.assertEquals(false, sub.meetsCriteria(doc));
        
        sub = parser.parse("NoProperty=exists=false");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("StringProperty=exists=true");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
        sub = parser.parse("StringProperty=exists=false");
        Assert.assertEquals(false, sub.meetsCriteria(doc));
        
        sub = parser.parse("Sub.Property==subprop");
        Assert.assertEquals(true, sub.meetsCriteria(doc));
        
    }
    
}
