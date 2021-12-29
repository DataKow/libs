package org.datakow.configuration.rabbit.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Annotation that creates the necessary beans to connect to the RabbitMQ server
 * using the properties in the {@link RabbitConfigurationProperties}
 * 
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Import(RabbitCoreAutoConfiguration.class)
public @interface EnableRabbit {
    
}
