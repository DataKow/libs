package org.datakow.messaging.events.configuration;

import org.datakow.configuration.rabbit.configuration.EnableRabbit;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Annotation used to establish a connection to RabbitMQ and to create the
 * beans necessary to send events to the DATAKOW events messaging system.
 * <p>
 * The client beans are created in {@link EventsSenderConfiguration}
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@EnableRabbit
@Import(EventsSenderConfiguration.class)
public @interface EnableEventsSender {
    
}
