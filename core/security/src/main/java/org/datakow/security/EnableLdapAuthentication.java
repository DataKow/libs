package org.datakow.security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Annotation that enables a Spring Framework security authentication feature
 * that uses LDAP.
 * <p>
 * The LDAP configuration properties are here: {@link LdapAuthenticationProviderConfigurationProperties}
 * 
 * @author kevin.off
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Import(LdapAuthenticationConfiguration.class)
public @interface EnableLdapAuthentication {
    
}
