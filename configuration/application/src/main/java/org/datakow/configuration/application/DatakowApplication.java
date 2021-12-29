package org.datakow.configuration.application;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Base class that creates a Spring Boot application with an embedded tomcat.
 * Will start up on port 8080 or the port specified in the server.port property.
 * 
 * @author kevin.off
 */
@SpringBootApplication(exclude = {ErrorMvcAutoConfiguration.class, MongoAutoConfiguration.class, MongoDataAutoConfiguration.class, RabbitAutoConfiguration.class})
public abstract class DatakowApplication {
    
    /**
     * Starts the application up on server.port or 8080.
     * 
     * @param applicationClass The main class in the default package
     * @param args args
     */
    public static void run(Class<?> applicationClass, String[] args) {
        String port = System.getProperty("server.port");
        System.setProperty("datakow.application.port", port == null ? "8080" : port);
        new SpringApplicationBuilder(applicationClass).web(WebApplicationType.SERVLET).registerShutdownHook(true).run(args);
    }
    
}
