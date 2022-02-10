package org.datakow.configuration.application;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Creates a Spring Boot application that does not run a web server.
 * 
 * @author kevin.off
 */
@SpringBootApplication(exclude = {ErrorMvcAutoConfiguration.class, MongoAutoConfiguration.class, MongoDataAutoConfiguration.class, RabbitAutoConfiguration.class})
public abstract class DatakowNonWebApplication {
    
    /**
     * Starts the application
     * 
     * @param applicationClass The main class in the default package
     * @param args args
     */
    public static void run(Class<?> applicationClass, String[] args) {
        new SpringApplicationBuilder(applicationClass).web(WebApplicationType.NONE).registerShutdownHook(true).run(args);
    }
    
}
