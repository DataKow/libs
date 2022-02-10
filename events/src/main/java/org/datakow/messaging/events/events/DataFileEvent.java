package org.datakow.messaging.events.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

/**
 * An event that occurs when an LDM ingested file has been moved to the queue directory.
 * <p>
 * Once this has happened a file ingester may receive this event and move to the web service.
 * @author kevin.off
 */
public class DataFileEvent extends Event {
    
    private String filePath;

    /**
     * Creates a new data file event with a random uuid ID of type data file
     */
    public DataFileEvent(){
        this.eventId = UUID.randomUUID().toString();
        this.eventType= EventType.DATA_FILE;
    }
    
    /**
     * Creates a new data file event for a give action.
     * <p>
     * This action may be created, updated, or deleted
     * 
     * @param eventAction The action
     */
    public DataFileEvent(String eventAction){
        this.eventId = UUID.randomUUID().toString();
        this.eventAction = eventAction;
        this.eventType = EventType.DATA_FILE;
    }
    
    /**
     * Creates a new data file event for a given event action and the path of the file in question
     * 
     * @param eventAction The event action
     * @param filePath the path to the file
     */
    public DataFileEvent(String eventAction, String filePath){
        this.eventId = UUID.randomUUID().toString();
        this.eventAction = eventAction;
        this.eventType = EventType.DATA_FILE;
        this.filePath = filePath;
    }
    
    /**
     * Gets the path to the file that the event was for
     * 
     * @return the full path to the file
     */
    @JsonProperty("File-Path")
    public String getFilePath() {
        return filePath;
    }

    /**
     * Sets the path to the file that the event was for
     * 
     * @param filePath the full path to the file
     */
    @JsonProperty("File-Path")
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }


    
    
}
