package org.datakow.security.access;

/**
 *
 * @author kevin.off
 */
public class Write extends Action{
    
    public Write(){
        super();
    }
    
    public Write(String ... roles){
        super(roles);
    }
    
}
