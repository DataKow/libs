package org.datakow.fiql;

import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.LogicalNode;
import cz.jirutka.rsql.parser.ast.NoArgRSQLVisitorAdapter;
import cz.jirutka.rsql.parser.ast.Node;
import cz.jirutka.rsql.parser.ast.OrNode;
import org.datakow.core.components.DateUtil;
import static org.datakow.fiql.MongoFiqlVisiter.NUMBER_PATTERN;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.util.StringUtils;


/**
 * Part of the visitor pattern, this class is used to create a logical criteria object.
 * Each part of the query is visited by this class and the criteria object
 * is built piece by piece.
 * 
 * @author kevin.off
 */
public class SubscriptionFiqlVisiter extends NoArgRSQLVisitorAdapter<SubscriptionCriteria> {

    SubscriptionCriteriaOperatorAppender appender = new SubscriptionCriteriaOperatorAppender();
    
    private final String prefix;
    
    public SubscriptionFiqlVisiter(String prefix){
        if (!StringUtils.hasText(prefix)){
            this.prefix = "";
        }else{
            this.prefix = prefix.endsWith(".") ? prefix : prefix + ".";
        }
    }
    
    /**
     * Joins children nodes in an AND operation
     * 
     * @param node The node that contains children to be AND'ed
     * @return The resulting criteria object
     */
    @Override
    public SubscriptionCriteria visit(AndNode node) {
        return joinChildrenNodesInCriteria(node);
    }

    /**
     * Joins children nodes in an OR operation
     * 
     * @param node The node that contains children to be OR'ed
     * @return The resulting criteria object
     */
    @Override
    public SubscriptionCriteria visit(OrNode node) {
        return joinChildrenNodesInCriteria(node);
    }

    /**
     * Applies the comparison operator to the node and creates a criteria object.
     * 
     * @param node The comparison node to convert
     * @return The resulting criteria object
     */
    @Override
    public SubscriptionCriteria visit(ComparisonNode node) {
        return createCriteria(node);
    }


    /**
     * Applies the comparison operator to the node and creates a criteria object
     *
     * @param node The comparison node to extract selector and argument from.
     * @return The resulting criteria object for the given comparison.
     */
    protected SubscriptionCriteria createCriteria(ComparisonNode node) {
        SubscriptionCriteria criteria = SubscriptionCriteria.where(extractCriteriaField(node));
        return appender.apply(node.getOperator(), criteria, extractArguments(node));
    }

    /**
     * Returns the property name from the node
     * 
     * @param node The node
     * @return The property name
     */
    private String extractCriteriaField(ComparisonNode node) {
        if (node.getSelector().startsWith("/")){
            return node.getSelector().replace("/", "");
        }else{
            return prefix + node.getSelector();
        }
    }

    /**
     * Returns the comparison value from the node
     * 
     * @param node The node
     * @return The comparison value
     */
    private Object extractArguments(ComparisonNode node) {
        if (node.getArguments().size() == 1){
            return castType(node.getArguments().get(0));
        }else{
            return node.getArguments()
                    .stream()
                    .map(o->castType(o))
                    .collect(Collectors.toList());   
        }
    }

    /**
     * Parses the value object and converts the type to the proper type.
     * If the argument looks like a number or date it is converted.
     * If the argument contains the cast operators String::, Number::, and Date::.
     * Also implements the Date::NOW.
     * 
     * @param orig The argument to convert
     * @return The converted argument
     */
    Object castType(Object orig){
        
        if (orig instanceof String){
            String str = (String)orig;
            if (str.startsWith("String::")){
                
                 return str.replace("String::", "");
                 
            }else if(str.startsWith("Number::")){
                
                 return Double.parseDouble(str.replace("Number::", ""));
                 
            }else if(str.startsWith("Date::")){
                if (str.equalsIgnoreCase("Date::NOW")){
                    return new Date();
                }else{
                    Date date = DateUtil.parseString(str.replace("Date::", ""));
                    if (date != null){
                        return date;
                    }
                }
                
            }else if (NUMBER_PATTERN.matcher((String)orig).matches()){
                
                return Double.parseDouble((String)orig);
                
            }else{
                
                Date date = DateUtil.parseString((String)orig);
                if (date != null){
                    return date;
                }else if(((String)orig).equalsIgnoreCase("true")){
                    return true;
                }else if (((String)orig).equalsIgnoreCase("false")){
                    return false;
                }else if (((String)orig).equalsIgnoreCase("null")){
                    return null;
                }
                
            }
        }
        return orig;
    }
    

    /**
     * Takes a logical node that contains children and creates an appropriate criteria
     * object that represents the node.
     * 
     * @param node The node to convert
     * @return The resulting criteria object
     */
    private SubscriptionCriteria joinChildrenNodesInCriteria(LogicalNode node) {
        SubscriptionCriteria criteria = new SubscriptionCriteria();
        List<SubscriptionCriteria> criteriaChain = new ArrayList<>();
        for (Node childNode : node) {
            criteriaChain.add(childNode.accept(this));
        }
        if (node instanceof OrNode) {
            criteria.or(criteriaChain.toArray(new SubscriptionCriteria[criteriaChain.size()]));
        } else {
            criteria.and(criteriaChain.toArray(new SubscriptionCriteria[criteriaChain.size()]));
        }
        return criteria;
    }
    
}
