package org.datakow.configuration.rabbit;

import org.datakow.configuration.rabbit.configuration.ExclusiveLockConfigurationProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * A mechanism to acquire an Exclusive Lock by using RabbitMQ's exclusive queues.
 * This class must be enabled by using the {@link org.datakow.configuration.rabbit.configuration.EnableExclusiveLock}.
 * This class will be available as a bean.
 * 
 * @author kevin.off
 */
@Component
@EnableScheduling
public class ExclusiveLock {
    
    @Autowired
    private RabbitAdmin rabbitAdmin;
    
    @Autowired
    ExclusiveLockConfigurationProperties props;
    
    private boolean locked = false;
    

    public boolean isLockAcquired(){
        return locked;
    }
    
    @Scheduled(fixedDelayString = "${datakow.rabbit.lock.lockAcquisionAttemptDelayInMs}")
    public void tryToAcquireLock(){
        
        try{                                                                        //durable exclusive autodelete
            rabbitAdmin.declareQueue(new Queue("q." + props.getLockName() + ".lock", false, true, false));
            this.locked = true;
        }catch(Exception e){
            this.locked = false;
        }
        
    }
    
}
