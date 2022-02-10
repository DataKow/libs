package org.datakow.messaging.events;

import org.datakow.messaging.events.events.Event;


/**
 * The interface that the client needs to implement in order to receive events.
 * All events will come to this implementation.
 * 
 * @author kevin.off
 */
public interface CatalogEventsReceiver {
 
    /**
     * Method to implement that receives all of the events
     * 
     * @param event The event that was received
     */
    public void receiveEvent(Event event);
    
}
