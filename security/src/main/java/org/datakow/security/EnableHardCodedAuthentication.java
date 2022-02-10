package org.datakow.security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Annotation that enables a Spring Framework security authentication feature
 * that requires the client to implement {@link HardCodedUserConfiguration}.
 * <p>
 * It is called hard coded because the get HardCodedUserConfiguration.getUsers method
 * returns all details including the password of the users.
 * 
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Import(HardCodedAuthenticationConfiguration.class)
public @interface EnableHardCodedAuthentication {
    
}
