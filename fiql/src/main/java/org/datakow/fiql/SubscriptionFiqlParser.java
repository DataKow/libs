package org.datakow.fiql;

import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.Node;


/**
 * FIQL parser used to convert a FIQL query to a {@link SubscriptionCriteria} object.
 * This object can then be used to compare DotNotationMap objects to determine if
 * they meet the criteria of the Query.
 * 
 * @author kevin.off
 */
public class SubscriptionFiqlParser {
    
    private final RSQLParser rsqlParser = new RSQLParser(FiqlOperator.subscriptionOperators());
    
    public SubscriptionCriteria parse(String fiql){
        return parse(fiql, "");
    }
    
    /**
     * Parses the fiql query and converts it to a criteria object
     * 
     * @param fiql The Fiql query to parse
     * @param prefix Prefix to use for every property name
     * @return The criteria object to use
     */
    public SubscriptionCriteria parse(String fiql, String prefix){
        
        Node rootNode = rsqlParser.parse(fiql);
        SubscriptionFiqlVisiter visiter = new SubscriptionFiqlVisiter(prefix);
        SubscriptionCriteria criteria = rootNode.accept(visiter);
        return criteria;
    }
    
}
