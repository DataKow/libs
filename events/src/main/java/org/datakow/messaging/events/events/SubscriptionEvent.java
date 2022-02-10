package org.datakow.messaging.events.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * An event that is fired when a Subscription is created, modified, or unsubscribed.
 * 
 * routing key
 * org.datakow.version.subscription-ws.env.subscription.subscribe|unsubscribe|pause|resume.id.created|updated|deleted
 * 
 * Event Body
 * {
 *   "subscriptionId":"2f0b7458-45c3-40c5-b531-c0d6f623e76e",
 *   "eventId":"2f0b7458-45c3-40c5-b531-c0d6f623e76e",
 *   "eventType":"SUBSCRIPTION",
 *   "subscriptionEvent":"SUBSCRIBE|UNSUBSCRIBE|PAUSE|RESUME",
 *   "eventAction":"created|updated|deleted"
 * }
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SubscriptionEvent extends Event {
    
    private String subscriptionId;
    private String subscriptionAction;
    private String endpointIdentifier;
    
    /**
     * Creates a new instance with a UUID id and an event type of SUBSCRIPTION
     */
    public SubscriptionEvent(){
        this.eventId = UUID.randomUUID().toString();
        this.eventType = EventType.SUBSCRIPTION;
    }
    
    /**
     * Creates a new Subscription Event for a subscription, subscription action, event action and an endpoint identifier
     * 
     * @param subscriptionId The ID of the subscription
     * @param subscriptionAction The action taken on the subscription
     * @param eventAction The event action
     * @param endpointIdentifier The identifier of the endpoint that the subscription is for
     */
    public SubscriptionEvent(String subscriptionId, String subscriptionAction, String eventAction, String endpointIdentifier){
        this.subscriptionId = subscriptionId;
        this.eventId = UUID.randomUUID().toString();
        this.eventType = EventType.SUBSCRIPTION;
        this.eventAction = eventAction;
        this.subscriptionAction = subscriptionAction;
        this.endpointIdentifier = endpointIdentifier;
    }

    /**
     * Gets the subscription id
     * 
     * @return The subscription id
     */
    @JsonProperty("SubscriptionId")
    public String getSubscriptionId() {
        return subscriptionId;
    }

    /**
     * Sets the subscription id
     * 
     * @param subscriptionId The subscription id
     */
    @JsonProperty("SubscriptionId")
    public void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    /**
     * Gets the Subscription Action
     * 
     * @return  The subscription action
     */
    @JsonProperty("Subscription-Action")
    public String getSubscriptionAction() {
        return subscriptionAction;
    }
    
    /**
     * Sets the Subscription Action
     * 
     * @param subscriptionAction The subscription action
     */
    @JsonProperty("Subscription-Action")
    public void setSubscriptionAction(String subscriptionAction) {
        this.subscriptionAction = subscriptionAction;
    }
    
    /**
     * Gets the identifier of the endpoint that the subscription is for
     * 
     * @return The endpoint identifier
     */
    @JsonProperty("Endpoint-Identifier")
    public String getEndpointIdentifier() {
        return endpointIdentifier;
    }
    
    /**
     * Sets the identifier of the endpoint that the subscription is for
     * 
     * @param endpointIdentifier The endpoint identifier
     */
    @JsonProperty("Endpoint-Identifier")
    public void setEndpointIdentifier(String endpointIdentifier) {
        this.endpointIdentifier = endpointIdentifier;
    }
    
}
