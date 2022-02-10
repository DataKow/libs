package org.datakow.core.components;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A date and calendar utility class used to make sure that all dates
 * are parsed and serialized the same way.
 * <p>
 * The output string format is yyyy-MM-dd'T'HH:mm:ssZ
 * <p>
 * The string parser accepts date strings using this regex:
 * <p>
 * ^(\\d{4})-?(\\d{2})-?(\\d{2})[T ]?(\\d{2}):?(\\d{2}):?(\\d{2})(\\.\\d{3})?(Z|[+-][\\d:]{1,5})?$
 * 
 * @author kevin.off
 */
public class DateUtil {
    
    public final static String simpleDateFormatString = "yyyy-MM-dd'T'HH:mm:ssZ";
    private final static Pattern dateFormatRegex = Pattern.compile("^(\\d{4})-?(\\d{2})-?(\\d{2})[T ]?(\\d{2}):?(\\d{2}):?(\\d{2})(\\.\\d{3})?(Z|[+-][\\d:]{1,5})?$");
    /**
     * Parses a date, converts the time to UTC, and returns a formatted string.
     * 
     * @param date The date object to convert
     * @return The formatted date string: yyyy-MM-dd'T'HH:mm:ssZ
     */
    public static String dateToUTCString(Date date){
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("GMT")));
        calendar.setTime(date);
        return calendarToString(calendar);
    }
    
    /**
     * Parses the Date object and moves the time to the given time zone then serializes the string.
     * 
     * @param date The date to parse
     * @param timeZone The time zone to convert to
     * @return The formatted date string: yyyy-MM-dd'T'HH:mm:ssZ
     */
    public static String dateToString(Date date, TimeZone timeZone){
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTime(date);
        return calendarToString(calendar);
    }
    
    /**
     * Parses a Calendar object, converts the time to UTC, and returns a formatted string.
     * 
     * @param calendar The date object to convert
     * @return The formatted date string: yyyy-MM-dd'T'HH:mm:ssZ
     */
    public static String calendarToString(Calendar calendar){
        SimpleDateFormat format = new SimpleDateFormat(simpleDateFormatString);
        format.setTimeZone(calendar.getTimeZone());
        
        String returnString = format.format(calendar.getTime());
        if (returnString.endsWith("+0000")){
            returnString = returnString.replace("+0000", "Z");
        }
        return returnString;
    }
    
    private static SimpleDateFormat makeFormat(TimeZone tz){
        SimpleDateFormat format = new SimpleDateFormat(simpleDateFormatString);
        format.setTimeZone(tz);
        return format;
    }
    
    /**
     * Parses a string that represents a date and converts it to a Date object.
     * 
     * @param date The date string
     * @return The converted Date object or null if it won't parse
     */
    public static Date parseString(String date){
        
        Matcher matcher = dateFormatRegex.matcher(date.trim());
        
        String formattedString = formatStringForParse(matcher);
        
        if (formattedString != null){
            SimpleDateFormat format = new SimpleDateFormat(simpleDateFormatString);
            try{
                Date d = format.parse(formattedString);
                return d;
            }catch(ParseException e){
                Logger.getLogger(DateUtil.class.getName()).log(Level.SEVERE, "Could not parse " + date + " into a date.", e);
                return null;
            }
        }else{
            return null;
        }
    }
    
    /**
     * Formats a string by parsing its parts and re-assembling it in a way
     * that the date parser can convert it to a Date Object.
     * 
     * @param matcher The regex matcher used to match the pattern of the date
     * @return The modified date string to use for the conversion
     */
    private static String formatStringForParse(Matcher matcher){
        
        if (matcher.matches()){
            String formattedString = matcher.group(1) + "-" + 
                                     matcher.group(2) + "-" + 
                                     matcher.group(3) + "T" + 
                                     matcher.group(4) + ":" + 
                                     matcher.group(5) + ":" + 
                                     matcher.group(6);
            
            String timeZoneString = formatTimeZoneStringForParse(matcher.group(8));
            
            formattedString = formattedString + timeZoneString;
            return formattedString;
        }else{
            return null;
        }
    }
    
    /**
     * Formats the time zone part of the date string so the date conversion can 
     * read it properly.
     * 
     * @param timeZoneString The string representing the time zone
     * @return The new formatted time zone string
     */
    static String formatTimeZoneStringForParse(String timeZoneString){
        if (timeZoneString == null || timeZoneString.isEmpty() || timeZoneString.equals("Z")){
            timeZoneString = "+0000";
        }else{
            //Remove the ':' and 'Z' just in case it might have been +00:00Z or something like that
            timeZoneString = timeZoneString.replace(":", "").replace("Z", "");
            //(-0600 | -600 | -06 | -6)
            String numbers = timeZoneString.substring(1);
            String sign = Character.toString(timeZoneString.charAt(0));
            if (numbers.length() == 3 && numbers.endsWith("00")){
                numbers = "0" + numbers;
            }else if (numbers.length() == 2 && numbers.startsWith("0")){
                numbers = numbers + "00";
            }else if (numbers.length() == 1){
                numbers = "0" + numbers + "00";
            }
            timeZoneString = sign + numbers;
        }
        return timeZoneString;
    }
    
    /**
     * Checks if the string actually represents a date and that there is no
     * other data in the string. 
     * <p>
     * The string has to match exactly and may not contain any other characters
     * 
     * @param dateString The string to check
     * @return true if it can be parsed
     */
    public static Boolean isFormattedDate(String dateString){
        return dateFormatRegex.matcher(dateString).matches();
    }
    
    /**
     * Determines if the given string contains a sequence of characters can be
     * matched against the date regex
     * @param dateString The string to check
     * @return true if the string contains a match somewhere
     */
    public static Boolean containsFormattedDate(String dateString){
        return dateFormatRegex.matcher(dateString).find();
    }
    
    
}
