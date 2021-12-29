package org.datakow.fiql;

import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.Node;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Class used to parse a FIQL query and to convert it to a Mongo {@link Criteria} object.
 * <p>
 * You may also use a prefix to apply to the property name of all properties used.
 * 
 * @author kevin.off
 */
public class MongoFiqlParser {
    
    private final RSQLParser rsqlParser = new RSQLParser(FiqlOperator.dbOperators());
    
    /**
     * Creates a new instance of the parser with no prefix.
     * Parses the fiql and returns the resulting Criteria
     * 
     * @param fiql The fiql string to parse
     * @return The parsed Criteria object
     */
    public Criteria parse(String fiql){
        return parse(fiql, "");
    }
    
    /**
     * Creates a new Mongo FIQL parser with a prefix that is assumed for all
     * documents that are being compared against. 
     * Parses the fiql and returns the resulting Criteria
     * 
     * @param fiql The fiql string to parse
     * @param fieldPrefix The prefix to apply to all properties
     * @return The parsed Criteria object
     */
    public Criteria parse(String fiql, String fieldPrefix){
        
        Node rootNode = rsqlParser.parse(fiql);
        MongoFiqlVisiter visiter = new MongoFiqlVisiter(fieldPrefix);
        Criteria criteria = rootNode.accept(visiter);
        return criteria;
    }
    
}
