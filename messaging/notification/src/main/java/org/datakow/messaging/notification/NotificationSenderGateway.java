// /*
//  * To change this license header, choose License Headers in Project Properties.
//  * To change this template file, choose Tools | Templates
//  * and open the template in the editor.
//  */
// package org.datakow.messaging.notification;

// import org.datakow.messaging.notification.notifications.Notification;
// import java.util.List;

// /**
//  * Interface that spring implements and wires into the integration framework.
//  * The client needs to Autowire a bean of this type in order to begin sending notifications.
//  * 
//  * @author kevin.off
//  */
// public interface NotificationSenderGateway {

//     /**
//      * 
//      * @param notification The notification to send
//      * @param bcc The list of queues to send the notification to
//      * @param requestId The requestId for logging
//      * @param correlationId The correlationId for logging
//      */
//     public void sendNotification(Notification notification, List<String> bcc, String requestId, String correlationId);
    
// }
