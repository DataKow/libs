package org.datakow.fiql;

/**
 * Enumerations used in {@link SubscriptionCriteria} to define the operations
 * that can be performed.
 * 
 * @author kevin.off
 */
public class ComparisonOperator {
    public static final String LT = "lt";
    public static final String GT = "gt";
    public static final String LTE = "lte";
    public static final String GTE = "gte";
    public static final String NE = "ne";
    public static final String IN = "in";
    public static final String NIN = "nin";
    public static final String ALL = "all";
    public static final String LIKE = "like";
    public static final String EXISTS = "exists";
    public static final String MATCH = "matches";
}
