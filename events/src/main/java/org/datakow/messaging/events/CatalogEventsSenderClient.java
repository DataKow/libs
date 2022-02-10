package org.datakow.messaging.events;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.datakow.configuration.rabbit.RabbitClient;
import org.datakow.messaging.events.configuration.EventsSenderConfiguration.CatalogEventsSenderGateway;
import org.datakow.messaging.events.events.CatalogEvent;
import org.datakow.messaging.events.events.Event;
import org.datakow.messaging.events.events.RecordAssociationEvent;
import org.datakow.messaging.events.events.RecordEvent;
import org.datakow.messaging.events.events.SubscriptionEvent;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

/**
 * The client bean used to send events to the DATAKOW messaging system.
 * 
 * @author kevin.off
 */
public class CatalogEventsSenderClient {
    
    @Autowired
    private CatalogEventsSenderGateway gateway;    
    
    @Autowired
    private EventsRoutingKeyBuilder builder;
    
    /**
     * Used to send an event to RabbitMQ
     * 
     * @param event The event to send
     * @param eventSubject The subject of the event used in the routing key
     * @param eventDetail The detail of the event used in the routing key
     */
    public void sendEvent(Event event, String eventSubject, String eventDetail){
        String correlationId = ThreadContext.get("correlationId");
        String requestId = UUID.randomUUID().toString();
        if (!StringUtils.hasText(correlationId)){
            correlationId = UUID.randomUUID().toString();
            ThreadContext.put("correlationId", correlationId);
        }
        ThreadContext.put("subRequestId", requestId);
        String oldCatalogIdentifier = ThreadContext.get("catalogIdentifier");
        String oldRecordIdentifier = ThreadContext.get("recordIdentifier");
        setThreadContext(event);
        
        try{
            Logger.getLogger(CatalogEventsSenderClient.class.getName()).log(Level.INFO, "Sending event: {0}", event.toJson());
        }catch(JsonProcessingException ex){
            Logger.getLogger(CatalogEventsSenderClient.class.getName()).log(Level.SEVERE, "Error converting event to JSON while logging for send", ex);
        }
        
        try{
            gateway.sendEvent(event, builder.buildRoutingKey(event.getEventType(), eventSubject, eventDetail, event.getEventAction()), requestId, correlationId);
        }finally{
            ThreadContext.remove("subRequestId");
            if (oldCatalogIdentifier == null){
                ThreadContext.remove("catalogIdentifier");
            }else{
                ThreadContext.put("catalogIdentifier", oldCatalogIdentifier);
            }
            if (oldRecordIdentifier == null){
                ThreadContext.remove("recordIdentifier");
            }else{
                ThreadContext.put("recordIdentifier", oldRecordIdentifier);
            }
        }
    }
    
    /**
     * Used to send an event to RabbitMQ
     * 
     * @param event The event to send
     * @param serviceName The name of the service that produced the event
     * @param eventSubject The subject of the event used in the routing key
     * @param eventDetail The detail of the event used in the routing key
     */
    public void sendEvent(Event event, String serviceName, String eventSubject, String eventDetail){
        String correlationId = ThreadContext.get("correlationId");
        String requestId = UUID.randomUUID().toString();
        if (correlationId == null || correlationId.isEmpty()){
            correlationId = UUID.randomUUID().toString();
            ThreadContext.put("correlationId", correlationId);
        }
        ThreadContext.put("subRequestId", requestId);
        String oldCatalogIdentifier = ThreadContext.get("catalogIdentifier");
        String oldRecordIdentifier = ThreadContext.get("recordIdentifier");
        setThreadContext(event);
        
        try{
            Logger.getLogger(CatalogEventsSenderClient.class.getName()).log(Level.INFO, "Sending event: {0}", event.toJson());
        }catch(JsonProcessingException ex){
            Logger.getLogger(CatalogEventsSenderClient.class.getName()).log(Level.SEVERE, "Error converting event to JSON while logging for send", ex);
        }
        
        try{
            gateway.sendEvent(event, builder.buildRoutingKey(serviceName, event.getEventType(), eventSubject, eventDetail, event.getEventAction()), requestId, correlationId);
        }finally{
            ThreadContext.remove("subRequestId");
            if (oldCatalogIdentifier == null){
                ThreadContext.remove("catalogIdentifier");
            }else{
                ThreadContext.put("catalogIdentifier", oldCatalogIdentifier);
            }
            if (oldRecordIdentifier == null){
                ThreadContext.remove("recordIdentifier");
            }else{
                ThreadContext.put("recordIdentifier", oldRecordIdentifier);
            }
        }
    }
    
    private void setThreadContext(Event event){
        try{
            if (event instanceof RecordEvent){
                ThreadContext.put("catalogIdentifier", ((RecordEvent)event).getCatalogIdentity().getCatalogIdentifier());
                ThreadContext.put("recordIdentifier", ((RecordEvent)event).getCatalogIdentity().getRecordIdentifier());
            }else if (event instanceof RecordAssociationEvent){
                ThreadContext.put("catalogIdentifier", ((RecordAssociationEvent)event).getObjectMetadataIdentity().getCatalogIdentifier());
                ThreadContext.put("recordIdentifier", ((RecordAssociationEvent)event).getObjectMetadataIdentity().getRecordIdentifier());
            }else if (event instanceof CatalogEvent){
                ThreadContext.put("catalogIdentifier", ((CatalogEvent)event).getCatalogIdentifier());
                ThreadContext.remove("recordIdentifier");
            }else if (event instanceof SubscriptionEvent){
                ThreadContext.put("catalogIdentifier", "subscriptions");
                ThreadContext.put("recordIdentifier", ((SubscriptionEvent)event).getSubscriptionId());
            }  
        }catch(Exception ex){
            Logger.getLogger(CatalogEventsSenderClient.class.getName()).log(Level.SEVERE, "Error setting ThreadContext when receiving event", ex);
        }
    }
    
}
