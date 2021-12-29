package org.datakow.fiql;

import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.LogicalNode;
import cz.jirutka.rsql.parser.ast.NoArgRSQLVisitorAdapter;
import cz.jirutka.rsql.parser.ast.Node;
import cz.jirutka.rsql.parser.ast.OrNode;
import org.datakow.core.components.DateUtil;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.util.StringUtils;

/**
 *
 * @author kevin.off
 */
public class MongoFiqlVisiter extends NoArgRSQLVisitorAdapter<Criteria> {

    private final String prefix;
    
    public MongoFiqlVisiter(String prefix){
        if (!StringUtils.hasText(prefix)){
            this.prefix = "";
        }else{
            this.prefix = prefix.endsWith(".") ? prefix : prefix + ".";
        }
    }
    
    MongoCriteriaOperatorAppender appender = new MongoCriteriaOperatorAppender();
    protected static final Pattern NUMBER_PATTERN = Pattern.compile("^-?\\d+(\\.\\d+)?$");
    
    @Override
    public Criteria visit(AndNode node) {
        return joinChildrenNodesInCriteria(node);
    }

    @Override
    public Criteria visit(OrNode node) {
        return joinChildrenNodesInCriteria(node);
    }

    @Override
    public Criteria visit(ComparisonNode node) {
        return createCriteria(node);
    }


    /**
     * Creates a field criteria for the given node and operator.
     *
     * @param node The comparison node to extract selector and argument from.
     * @return A field criteria for the given comparison.
     *
     */
    protected Criteria createCriteria(ComparisonNode node) {
        Criteria criteria = Criteria.where(extractCriteriaField(node));
        return appender.apply(node.getOperator(), criteria, extractArguments(node));
    }
    
    /**
     * Parses the Comparison node, apply the prefix, and returns the property name.
     * 
     * @param node The comparison node to parse
     * @return The property name including the prefix
     */
    private String extractCriteriaField(ComparisonNode node) {
        if (node.getSelector().startsWith("/")){
            return node.getSelector().replace("/", "");
        }else{
            return prefix + node.getSelector();
        }
    }

    /**
     * Parses the comparison node and extracts the value or list of values from it.
     * 
     * @param node The node to check
     * @return The value
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
                
                if (((String)orig).contains(".")){
                    return Double.parseDouble((String)orig);
                }else{
                    return Long.parseLong((String)orig);
                }
                
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
     * Creates a new criteria object and adds it to the logical chain in the appropriate place.
     * <p>
     * And nodes are added to the AND list, or nodes are added to the OR list.
     * 
     * @param node The logical node to add a criteria object to.
     * @return The new Criteria object
     */
    private Criteria joinChildrenNodesInCriteria(LogicalNode node) {
        Criteria criteria = new Criteria();
        List<Criteria> criteriaChain = new ArrayList<>();
        for (Node childNode : node) {
            criteriaChain.add(childNode.accept(this));
        }
        if (node instanceof OrNode) {
            criteria.orOperator(criteriaChain.toArray(new Criteria[criteriaChain.size()]));
        } else {
            criteria.andOperator(criteriaChain.toArray(new Criteria[criteriaChain.size()]));
        }
        return criteria;
    }
    
}
