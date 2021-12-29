package org.datakow.messaging.notification;


import com.fasterxml.jackson.core.JsonProcessingException;

import org.datakow.configuration.rabbit.RabbitClient;
import org.datakow.messaging.notification.configuration.NotificationSenderConfiguration.NotificationSenderGateway;
import org.datakow.messaging.notification.notifications.Notification;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;

 /**
 * The client bean used to send notifications to the DATAKOW messaging system.
 * 
 * @author kevin.off
 */
public class NotificationSenderClient {
    
    @Autowired
    private NotificationSenderGateway gateway;
    
    /**
     * Used to send a notification to RabbitMQ
     * 
     * @param notification The notification to send
     * @param bcc the list of queue names to send the notification to
     */
    public void sendNotification(Notification notification, List<String> bcc){
        String correlationId = ThreadContext.get("correlationId");
        String requestId = UUID.randomUUID().toString();
        if (correlationId == null || correlationId.isEmpty()){
            correlationId = UUID.randomUUID().toString();
            ThreadContext.put("correlationId", correlationId);
        }
        ThreadContext.put("subRequestId", requestId);
        String oldCatalogIdentifier = ThreadContext.get("catalogIdentifier");
        String oldRecordIdentifier = ThreadContext.get("recordIdentifier");
        ThreadContext.put("catalogIdentifier", notification.getObjectMetadataIdentity().getCatalogIdentifier());
        ThreadContext.put("recordIdentifier", notification.getObjectMetadataIdentity().getRecordIdentifier());
        
        try{
            Logger.getLogger(NotificationSenderClient.class.getName()).log(Level.INFO, "Sending notification: {0} to {1}",new Object[]{notification.toJson(), bcc});
        }catch(JsonProcessingException ex){
            Logger.getLogger(NotificationSenderClient.class.getName()).log(Level.SEVERE, "Error converting notification to JSON while logging for send", ex);
        }
        
        try{
            gateway.sendNotification(notification, bcc, requestId, correlationId);
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
    
}
