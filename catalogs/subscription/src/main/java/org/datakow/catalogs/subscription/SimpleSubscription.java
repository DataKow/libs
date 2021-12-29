package org.datakow.catalogs.subscription;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.DatakowObjectMapper;
import org.datakow.fiql.SubscriptionCriteria;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Base class for a basic Subscription that handles SimpleCriteria.
 * These criteria have no internal evaluator so retrieving them based on
 * their criteria will need to be handled by the implementation.
 * 
 * @author kevin.off
 */
@JsonIgnoreProperties(ignoreUnknown = true, value = { "_id" })
public abstract class SimpleSubscription {

    protected String id;
    protected String catalogIdentifier;
    protected String action = "associated";
    private SubscriptionCriteria criteria;
    protected String userName;
    protected boolean paused = false;
    private String endpointIdentifier = "AMQP";
    
    private Map<String, String> properties = new HashMap<>();
    
    /**
     * Gets the ID of the subscription
     * 
     * @return The ID
     */
    public String getId() {
        return this.id;
    }

    /**
     * Sets the ID of the subscription
     * 
     * @param id The ID
     */
    public void setId(String id) {
        this.id = id;
    }
    
    /**
     * Gets the catalog identifier of the subscription
     * 
     * @return The catalog identifier
     */
    public String getCatalogIdentifier(){
        return this.catalogIdentifier;
    }
    
    /**
     * Sets the catalog identifier of the subscription
     * 
     * @param id The catalog identifier
     */
    public void setCatalogIdentifier(String id){
        if (id != null && id.isEmpty()){
            this.catalogIdentifier = null;
        }else{
            this.catalogIdentifier = id;
        }
    }
    
    /**
     * Gets the catalog action that the subscription is interested in.
     * <p>
     * The action can be created or associated
     * 
     * @return The catalog action
     */
    public String getCatalogAction(){
        return this.action;
    }
    
    /**
     * Sets the catalog action that the subscription is interested in.
     * <p>
     * The action can be created or associated
     * 
     * @param catalogAction The catalog action
     */
    public final void setCatalogAction(String catalogAction){
        if (catalogAction == null || catalogAction.isEmpty()){
            this.action = "associated";
        }else{
            this.action = catalogAction;
        }
    }
    
    /**
     * Sets the internal subscription criteria object to use to compare metadata records with
     * 
     * @param criteria The criteria to use to compare
     */
    @JsonIgnore
    public void setCriteria(SubscriptionCriteria criteria){
        this.criteria = criteria;
    }
    
    /**
     * gets the internal subscription criteria object to use to compare metadata records with
     * 
     * @return The criteria to use to compare
     */
    @JsonIgnore
    public SubscriptionCriteria getCriteria(){
        return this.criteria;
    }
    
    /**
     * Sets the username of the subscription
     * 
     * @param userName The username
     */
    public void setUserName(String userName){
        this.userName = userName;
    }
    
    /**
     * Gets the username of the subscription
     * 
     * @return The username
     */
    public String getUserName(){
        return userName;
    }
    
    /**
     * Compares the metadata document against the {@link SubscriptionCriteria} to see
     * if the metadata record meets the criteria of the SubscriptionCriteria object.
     * <p>
     * The document meets the criteria if the criteria is null or if the metadata
     * is not null and meets the criteria in the SubscriptionCriteria object.
     * 
     * @param metadata The metadata record to check.
     * @return true if the record meets the criteria
     */
    public boolean meetsCriteria(DotNotationMap metadata) {
        return criteria == null || (metadata != null && criteria.meetsCriteria(metadata));
    }
    
    /**
     * Sets the paused property indicating that the subscription is currently active but not being listened to.
     * 
     * @param paused true if the subscription is paused
     */
    public void setPaused(boolean paused){
        this.paused = paused;
    }
    
    /**
     * Gets the paused property indicating that the subscription is currently active but not being listened to.
     * 
     * @return true if the subscription is paused
     */
    public boolean getPaused(){
        return this.paused;
    }

    /**
     * Gets the name of the endpoint that is servicing the subscription.
     * 
     * @return the name of the endpoint
     */
    public String getEndpointIdentifier() {
        return endpointIdentifier;
    }

    /**
     * Sets the name of the endpoint that is servicing the subscription.
     * 
     * @param endpointIdentifier the name of the endpoint
     */
    public final void setEndpointIdentifier(String endpointIdentifier) {
        if (endpointIdentifier == null || endpointIdentifier.isEmpty()){
            this.endpointIdentifier = "AMQP";
        }else{
            this.endpointIdentifier = endpointIdentifier;
        }
    }
    
    /**
     * Agreed upon queue name schema that is based on the SubscriptionId
     * 
     * @return The queue name for this subscription
     */
    @JsonIgnore
    public String getQueueName(){
        return "q.subscriber." + this.getId();
    }
    
    /**
     * Agreed upon routing key name schema that is based on the SubscriptionId
     * 
     * @return The routing key. In this case it will be on a direct exchange so the
     * routing key = the queue name
     */
    @JsonIgnore
    public String getRoutingKey(){
        return this.getQueueName();
    }    
    
    /**
     * Gets the map of additional subscription properties
     * 
     * @return the properties
     */
    public Map<String, String> getProperties() {
        return this.properties;
    }

    /**
     * Sets the map of additional subscription properties
     * 
     * @param properties the properties
     */
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * Adds an additional property to the map of additional properties.
     * 
     * @param name The name of the property to set
     * @param value The value of the property
     */
    public final void setProperty(String name, String value) {
        this.properties.put(name, value);
    }

    /**
     * Gets an additional property by its name
     * 
     * @param name The name of the property
     * @return The property value or null
     */
    public String getProperty(String name) {
        return this.properties.get(name);
    }
    
    public String toJson() throws JsonProcessingException{
        ObjectMapper mapper = DatakowObjectMapper.getObjectMapper();
        String json = mapper.writeValueAsString(this);
        return json;
    }
    
    @Override
    public boolean equals(Object other){
        
        if(!SimpleSubscription.class.isInstance(other)){
            return false;
        }
        
        SimpleSubscription sub = (SimpleSubscription)other;
        boolean matchingCriteria = this.criteria.equals(sub.getCriteria());
        if (!matchingCriteria){
            return false;
        }
        int otherPropertyCount = sub.getProperties().size();
        int thisPropertyCount = this.getProperties().size();
        if (otherPropertyCount != thisPropertyCount){
            return false;
        }
        int matchingProperty = 0;
        for(Entry<String, String> otherProperty : sub.getProperties().entrySet()){
            if (this.getProperties().containsKey(otherProperty.getKey()) && 
                    this.getProperty(otherProperty.getKey()).equalsIgnoreCase(otherProperty.getValue().toString())){
                matchingProperty++;
            }
        }
        if (matchingProperty != thisPropertyCount || matchingProperty != otherPropertyCount){
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 79 * hash + Objects.hashCode(this.id);
        hash = 79 * hash + Objects.hashCode(this.criteria);
        hash = 79 * hash + Objects.hashCode(this.properties);
        return hash;
    }

    
}
