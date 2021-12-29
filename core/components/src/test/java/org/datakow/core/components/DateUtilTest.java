package org.datakow.core.components;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author kevin.off
 */
public class DateUtilTest {
    
    public DateUtilTest() {
    }

    private final String outputString = "2016-01-12T02:10:15Z";

    
    @Test
    public void testDateConversion() {
        List<String> testStrings = Arrays.asList(
                            "2016-01-12T02:10:15Z",
                            "2016-01-11T20:10:15-0600",
                            "2016-01-11T20:10:15-06:00",
                            "2016-01-11T20:10:15-600",
                            "2016-01-11 20:10:15-06:00",
                            "2016-01-11 20:10:15-6:00",
                            "2016-01-12 02:10:15",
                            "20160112T021015Z",
                            "20160112 021015",
                            "20160112021015",
                            "20160112T021015",
                            "20160111T201015-0600",
                            "20160111T201015-06",
                            "20160111 201015-0600",
                            "20160111 201015-6",
                            "20160111T201015-06:00");
        testStrings.stream().forEach(s->{
        
            Date d = DateUtil.parseString(s);
            Assert.assertNotNull(d);
            String created = DateUtil.dateToUTCString(d);
            Assert.assertEquals(outputString, created);
            
        });
        
    }

    
    
    @Test
    public void testFormatTimeZoneStringForParse() {
        
        List<String> testTimeZones = Arrays.asList("Z",
                                                   "-0600",
                                                   "-06:00",
                                                   "-6:00",
                                                   "-600",
                                                   "-06",
                                                   "-6"
                );
        testTimeZones.stream().forEach(s->{
        
            String created = DateUtil.formatTimeZoneStringForParse(s);
            if (s.equals("Z")){
                Assert.assertEquals("+0000", created);
            }else{
                Assert.assertEquals("-0600", created);
            }
            
        });
        
    }
    
    @Test
    public void testCalendarToString(){
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("CST"));
        cal.set(2017, 6, 19, 14, 53, 40);
        String str = DateUtil.calendarToString(cal);
        //2017-07-19T14:53:40-0500
        
        Assert.assertEquals("2017-07-19T14:53:40-0500", str);
        
        
    }

    @Test
    public void testDateToUTCString() {
    }

    @Test
    public void testDateToString() {
    }

    @Test
    public void testParseString() {
    }

    @Test
    public void testIsFormattedDate() {
    }

    @Test
    public void testContainsFormattedDate() {
    }
    
}
