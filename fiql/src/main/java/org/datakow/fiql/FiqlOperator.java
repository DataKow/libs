package org.datakow.fiql;

import cz.jirutka.rsql.parser.ast.ComparisonOperator;
import cz.jirutka.rsql.parser.ast.RSQLOperators;
import static cz.jirutka.rsql.parser.ast.RSQLOperators.defaultOperators;
import java.util.Set;

/**
 * Enumerations of all operations that can be performed in a fiql query.
 * 
 * @author kevin.off
 */
public abstract class FiqlOperator extends RSQLOperators {
    
    /*
    Default Operators
    EQUAL = new ComparisonOperator("=="),
    NOT_EQUAL = new ComparisonOperator("!="),
    GREATER_THAN = new ComparisonOperator("=gt=", ">"),
    GREATER_THAN_OR_EQUAL = new ComparisonOperator("=ge=", ">="),
    LESS_THAN = new ComparisonOperator("=lt=", "<"),
    LESS_THAN_OR_EQUAL = new ComparisonOperator("=le=", "<="),
    IN = new ComparisonOperator("=in=", true),
    NOT_IN = new ComparisonOperator("=out=", true);
    */
    
    public static final ComparisonOperator ALL = new ComparisonOperator("=all=", true);
    public static final ComparisonOperator LIKE = new ComparisonOperator("=like=");
    public static final ComparisonOperator WITHIN = new ComparisonOperator("=within=", true);
    public static final ComparisonOperator NEAR = new ComparisonOperator("=near=", true);
    public static final ComparisonOperator MATCH = new ComparisonOperator("=matches=", false);
    public static final ComparisonOperator INTERSECT = new ComparisonOperator("=intersects=", false);
    public static final ComparisonOperator EXISTS = new ComparisonOperator("=exists=", false);
    public static final ComparisonOperator TYPE = new ComparisonOperator("=type=", false);
    
    /**
     * Gets all operators that can be performed.
     * 
     * @return all operators that can be performed
     */
    public static Set<ComparisonOperator> dbOperators() {
        Set<ComparisonOperator> set = defaultOperators();
        set.add(ALL);
        set.add(LIKE);
        set.add(WITHIN);
        set.add(NEAR);
        set.add(MATCH);
        set.add(INTERSECT);
        set.add(EXISTS);
        set.add(TYPE);
        return set;
    }
    
    /**
     * All operations that can be performed that is supported by the
     * subscription criteria class.
     * 
     * @return operations that can be performed by a subscription
     */
    public static Set<ComparisonOperator> subscriptionOperators(){
        Set<ComparisonOperator> set = defaultOperators();
        set.add(ALL);
        set.add(LIKE);
        set.add(EXISTS);
        set.add(MATCH);
        return set;
    }
    
}
