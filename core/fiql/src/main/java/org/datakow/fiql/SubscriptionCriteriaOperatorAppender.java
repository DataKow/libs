package org.datakow.fiql;

import cz.jirutka.rsql.parser.ast.ComparisonOperator;

import java.util.HashMap;
import java.util.Map;


/**
 * This class is responsible for adding the comparison operator depending on the
 * type of comparison.
 * 
 * @author kevin.off
 */
public class SubscriptionCriteriaOperatorAppender {
    
    private static final Map<ComparisonOperator, OperatorApplication> operators = new HashMap<>();
    
    /**
     * Adds the appropriate comparison operation depending on the FiqlOperator
     */
    public SubscriptionCriteriaOperatorAppender(){
        operators.put( FiqlOperator.EQUAL,                 (criteria, arg)-> { return criteria.is(arg);  });
        operators.put( FiqlOperator.GREATER_THAN_OR_EQUAL, (criteria, arg)-> { return criteria.gte(arg); });
        operators.put( FiqlOperator.GREATER_THAN,          (criteria, arg)-> { return criteria.gt(arg);  });
        operators.put( FiqlOperator.LESS_THAN_OR_EQUAL,    (criteria, arg)-> { return criteria.lte(arg); });
        operators.put( FiqlOperator.LESS_THAN,             (criteria, arg)-> { return criteria.lt(arg);  });
        operators.put( FiqlOperator.NOT_EQUAL,             (criteria, arg)-> { return criteria.ne(arg);  });
        operators.put( FiqlOperator.IN,                    (criteria, arg)-> { return criteria.in(arg);  });
        operators.put( FiqlOperator.NOT_IN,                (criteria, arg)-> { return criteria.nin(arg); });
        operators.put( FiqlOperator.ALL,                   (criteria, arg)-> { return criteria.all(arg); });
        operators.put( FiqlOperator.LIKE,                  (criteria, arg)-> { return criteria.regex(arg.toString()); });
        operators.put( FiqlOperator.EXISTS,                (criteria, arg)-> { return criteria.exists(Boolean.valueOf(arg.toString())); });
        operators.put( FiqlOperator.MATCH,                 (criteria, arg)-> { return criteria.matches((String)arg); });
//        operators.put( FiqlOperator.WITHIN,                (criteria, arg)-> { 
//            Point point1 = new Point(Double.valueOf((String)((Object[])arg)[0]), Double.valueOf((String)((Object[])arg)[1]));
//            Point point2 = new Point(Double.valueOf((String)((Object[])arg)[2]), Double.valueOf((String)((Object[])arg)[3]));
//            return criteria.within(new Box(point1, point2));
//        });
//        operators.put( FiqlOperator.NEAR,                (criteria, arg)-> { 
//            Point point1 = new Point(Double.valueOf((String)((Object[])arg)[0]), Double.valueOf((String)((Object[])arg)[1]));
//            return criteria.near(point1).maxDistance(Double.valueOf((String)((Object[])arg)[2]));
//        });
    }
    
    /**
     * Retrieves the appropriate Comparison operator method and adds it to the
     * existing criteria object.
     * 
     * @param operator The comparison operator use apply
     * @param criteria The existing criteria to add it to
     * @param argument The value to use to compare it against
     * @return The resulting criteria object with the new comparison in it
     */
    public SubscriptionCriteria apply(ComparisonOperator operator, SubscriptionCriteria criteria, Object argument){
        OperatorApplication app = operators.get(operator);
        return app.apply(criteria, argument);
    }
    
    /**
     * Functional interface used to provide an apply method to call to add the
     * new criteria operation to a SubscriptionCriteria
     */
    @FunctionalInterface
    private interface OperatorApplication{
        SubscriptionCriteria apply(SubscriptionCriteria criteria, Object argument);
    }
    
}
