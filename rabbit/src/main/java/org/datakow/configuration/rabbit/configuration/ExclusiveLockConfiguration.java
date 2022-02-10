package org.datakow.configuration.rabbit.configuration;

import org.datakow.configuration.rabbit.ExclusiveLock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class enabled by {@link EnableExclusiveLock} to create the
 * {@link ExclusiveLock} bean.
 * 
 * @author kevin.off
 */
@Configuration
@EnableConfigurationProperties(ExclusiveLockConfigurationProperties.class)
public class ExclusiveLockConfiguration {
    
    @Bean
    public ExclusiveLock exclusiveLock(){
        return new ExclusiveLock();
    }
    
}
