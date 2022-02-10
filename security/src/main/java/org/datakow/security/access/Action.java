package org.datakow.security.access;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

/**
 *
 * @author kevin.off
 */
public abstract class Action {
    
    private final Collection<String> roles = new HashSet<>();
    
    public Action(){};
    public Action(String ... roles){
        this.roles.addAll(Arrays.asList(roles));
    }
    
    public Collection<String> getRoles() {
        return roles;
    }
    
    public void addRole(String role){
        if (!roles.contains(role)){
            roles.add(role);
        }
    }
    
    public void removeRole(String role){
        this.roles.remove(role);
    }
    
    @Override
    public String toString(){
        return this.roles.toString();
    }
    
}
