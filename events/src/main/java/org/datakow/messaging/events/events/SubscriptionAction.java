package org.datakow.messaging.events.events;

/**
 * An enumeration used to determine what happened to the subscription
 * that caused the event.
 * 
 * @author kevin.off
 */
public class SubscriptionAction {
    
    public static final String SUBSCRIBE = "subscribe";
    public static final String UNSUBSCRIBE = "unsubscribe";
    public static final String PAUSE = "pause";
    public static final String RESUME = "resume";
    
}
