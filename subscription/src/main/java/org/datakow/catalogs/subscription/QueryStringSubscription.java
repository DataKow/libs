package org.datakow.catalogs.subscription;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.jirutka.rsql.parser.ast.ComparisonOperator;
import org.datakow.core.components.DatakowObjectMapper;
import org.datakow.fiql.FiqlOperator;
import org.datakow.fiql.SubscriptionFiqlParser;
import java.io.IOException;


/**
 * Class that represents a subscription that is used to receive notifications.
 * 
 * @author kevin.off
 */ 
@JsonIgnoreProperties(ignoreUnknown = true, value = { "_id" })
public class QueryStringSubscription extends SimpleSubscription {
    
    private String qs;

    /**
     * Creates an instance of a subscription object
     */
    public QueryStringSubscription(){
        
    }
    
    /**
     * Creates an instance of a subscription object with all of the required fields.
     * 
     * @param subscriptionId The ID of the subscription
     * @param queryString The FIQL query to use to filter metadata
     * @param catalogIdentifier The catalog to subscribe to
     * @param catalogAction The action you are interested in (created|associated)
     * @param userName Your username
     * @param endpointIdentifier The name of the endpoint that will be servicing you subscription
     */
    public QueryStringSubscription(String subscriptionId, String queryString, String catalogIdentifier, String catalogAction, String userName, String endpointIdentifier){
        this.id = subscriptionId;
        this.setQueryString(queryString);
        this.catalogIdentifier = catalogIdentifier;
        this.setCatalogAction(catalogAction);
        this.userName = userName;
        this.setEndpointIdentifier(endpointIdentifier);
    }
    
    /**
     * Parses the FIQL query string and converts it to a {@link org.datakow.fiql.SubscriptionCriteria} object.
     * Then sets that value in this object.
     * 
     * @param queryString The FIQL string to parse and set
     */
    public final void setQueryString(String queryString){
        this.qs = queryString;
        if (queryString != null && !queryString.isEmpty()){
            if (queryString.startsWith("s=")){
                queryString = queryString.replace("s=", "");
            }
            if (!isFiql(queryString)){
                queryString = queryString.replace("&", ";").replace("=", "==");
            }
            this.setCriteria(new SubscriptionFiqlParser().parse(queryString));
        }
    }
    
    /**
     * Used to determine if a string is a FIQL query string or not
     * 
     * @param queryString The string to check
     * @return true if the string is a valid query string
     */
    public boolean isFiql(String queryString){
        boolean isFiql = false;
        for (ComparisonOperator operator : FiqlOperator.subscriptionOperators()){
            for(String op : operator.getSymbols()){
                if (queryString.contains(op)){
                    isFiql = true;
                    break;
                }
            }
            if (isFiql){
                break;
            }
            
        }
        return isFiql;
    }
    
    /**
     * Gets the original FIQL query string
     * @return The query string
     */
    public String getQueryString(){
        return qs;
    }
    
    @Override
    public String toJson() throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    public static QueryStringSubscription fromJson(String json) throws JsonProcessingException{
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowObjectMapper();
        QueryStringSubscription identity = mapper.readValue(json, QueryStringSubscription.class);
        return identity;
    }
    
    @Override
    public String toString(){
        try {
            return this.toJson();
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("Error converting Subscription to JSON", ex);
        }
    }
    
}
