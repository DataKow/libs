package org.datakow.configuration.application;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;

/**
 * Base class that creates a Spring Boot application with an embedded tomcat.
 * <p>
 * Will start up on the first available port between min and max.
 * Make sure you configure your logger to use the port number "datakow.application.port"
 * or the instance number "datakow.application.instanceNumber" in the log file name.
 * 
 * @author kevin.off
 */
@SpringBootApplication(exclude = {ErrorMvcAutoConfiguration.class, MongoAutoConfiguration.class, MongoDataAutoConfiguration.class, RabbitAutoConfiguration.class})
public abstract class DatakowMultiInstanceApplication implements WebServerFactoryCustomizer<TomcatServletWebServerFactory> {
    
    @Value("${datakow.application.port}")
    int applicationPort;
    
    /**
     * Starts the application up on a port between min and max.
     * 
     * @param applicationClass The main class in the default package
     * @param minPort The minimum port number
     * @param maxPort The maximum port number
     * @param args args
     */
    public static void run(Class<?> applicationClass, int minPort, int maxPort, String ... args){
        int myPort = PortSelector.getNextAvailablePort(minPort, maxPort);
        System.setProperty("datakow.application.instanceNumber", Integer.toString(myPort - minPort));
        System.setProperty("datakow.application.port", Integer.toString(myPort));
        new SpringApplicationBuilder(applicationClass).web(WebApplicationType.SERVLET).run(args);
    }
    
    /**
     * Sets the port that the application binds to
     * 
     * @param container This setvlet container to customize
     */
    @Override
    public void customize(TomcatServletWebServerFactory container) {
        container.setPort(applicationPort);
    }
    
}
