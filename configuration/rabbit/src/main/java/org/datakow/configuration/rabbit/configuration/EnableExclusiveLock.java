package org.datakow.configuration.rabbit.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Annotation that enables rabbit and creates the {@link org.datakow.configuration.rabbit.ExclusiveLock} bean.
 * 
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@EnableRabbit
@Import(ExclusiveLockConfiguration.class)
public @interface EnableExclusiveLock {
    
}
