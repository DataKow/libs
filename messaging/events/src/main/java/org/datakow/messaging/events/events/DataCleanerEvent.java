package org.datakow.messaging.events.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.UUID;

/**
 *
 * @author kevin.off
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataCleanerEvent extends Event {
    
    private String catalogIdentifier;
    private String fiql;
    
    public DataCleanerEvent(){
        this.eventId = UUID.randomUUID().toString();
        this.eventType= EventType.DATA_CLEANER;
    }

    public DataCleanerEvent(String catalogIdentifier, String fiql){
        this.eventId = UUID.randomUUID().toString();
        this.eventType= EventType.DATA_CLEANER;
        this.catalogIdentifier = catalogIdentifier;
        this.fiql = fiql;
    }
    
    public String getCatalogIdentifier() {
        return catalogIdentifier;
    }

    public void setCatalogIdentifier(String catalogIdentifier) {
        this.catalogIdentifier = catalogIdentifier;
    }

    public String getFiql() {
        return fiql;
    }

    public void setFiql(String fiql) {
        this.fiql = fiql;
    }
    
    
    
}
