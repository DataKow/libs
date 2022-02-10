package org.datakow.fiql;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * General purpose object converter.
 * Contains a bunch of conversion methods to use to convert objects from
 * one type to another.
 * 
 * @author kevin.off
 */
public class ObjectConverter {
    private static final Map<String, Method> CONVERTERS = new HashMap<>();
    
    static {
        // Preload converters.
        Method[] methods = ObjectConverter.class.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getParameterTypes().length == 1) {
                // Converter should accept 1 argument. This skips the convert() method.
                CONVERTERS.put(method.getParameterTypes()[0].getName() + "_"
                    + method.getReturnType().getName(), method);
            }
        }
    }
    
    /**
     * Private constructor
     */
    private ObjectConverter() {
        // Utility class, hide the constructor.
    }
    
    /**
     * Converts an object from their original class to the provided class.
     * First checks null, then attempts a simple cast, then uses the pre-defined 
     * conversion function.
     * 
     * @param <T> The type to convert to
     * @param from The object to convert
     * @param to The class to convert the value to
     * @return The resulting object
     */
    public static <T> T convert(Object from, Class<T> to) {

        // Null is just null.
        if (from == null || to == null) {
            return null;
        }
        
        // Can we cast? Then just do it.
        if (to.isAssignableFrom(from.getClass())) {
            return to.cast(from);
        }
        

        if (Number.class.isAssignableFrom(from.getClass()) && Number.class.isAssignableFrom(to)){
            
            switch(to.getSimpleName()){
                case "Byte":
                    return to.cast(((Number)from).byteValue());
                case "Double":
                    return to.cast(((Number)from).doubleValue());
                case "Float":
                    return to.cast(((Number)from).floatValue());
                case "Integer":
                    return to.cast(((Number)from).intValue());
                case "Long":
                    return to.cast(((Number)from).longValue());
                case "Short":
                    return to.cast(((Number)from).shortValue());
            }
        }
        
        // Lookup the suitable converter.
        String converterId = from.getClass().getName() + "_" + to.getName();
        Method converter = CONVERTERS.get(converterId);
        if (converter == null) {
            throw new UnsupportedOperationException("Cannot convert from " 
                + from.getClass().getName() + " to " + to.getName()
                + ". Requested converter does not exist.");
        }

        // Convert the value.
        try {
            return to.cast(converter.invoke(to, from));
        } catch (Exception e) {
            throw new RuntimeException("Cannot convert from " 
                + from.getClass().getName() + " to " + to.getName()
                + ". Conversion failed with " + e.getMessage(), e);
        }
    }
    
    public static Boolean integerToBoolean(Integer value) {
        return value == 0 ? Boolean.FALSE : Boolean.TRUE;
    }
    
    public static Integer booleanToInteger(Boolean value) {
        return value ? 1 : 0;
    }
    
    public static BigDecimal doubleToBigDecimal(Double value) {
        return new BigDecimal(value);
    }
    
    public static Double bigDecimalToDouble(BigDecimal value) {
        return value.doubleValue();
    }
    
    public static String integerToString(Integer value) {
        return value.toString();
    }
    
    public static String booleanToString(Boolean value) {
        return value.toString();
    }
    
    public static Boolean stringToBoolean(String value) {
        return Boolean.valueOf(value);
    }
    
    public static Double stringToDouble(String value){
        return Double.valueOf(value);
    }
    
    public static Integer stringToInteger(String value) {
        return Integer.valueOf(value);
    }
}
