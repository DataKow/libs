package org.datakow.messaging.notification;


import org.datakow.messaging.notification.notifications.Notification;


/**
 * The interface that the client needs to implement in order to receive notifications.
 * All notifications will come to this implementation.
 * 
 * @author kevin.off
 */
public interface NotificationReceiver {
 
    /**
     * Method to implement that receives all of the notifications
     * 
     * @param notification The notification that was received
     * @param subscriptionId the id of the subscription that is interested in the notification
     * @param queueName The name of the queue that the notification came from
     */
    public void receiveNotification(Notification notification, String subscriptionId, String queueName);
    
}
