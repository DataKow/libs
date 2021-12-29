package org.datakow.security.access;

import java.util.Collection;

/**
 *
 * @author kevin.off
 */
public class Realm {
    
    private Read read = new Read();
    private Write write = new Write();
    private final String name;

    public Realm(String name){
        this.name = name;
    }
    
    public Realm(String name, Read read, Write write){
        this.name = name;
        this.read = read;
        this.write = write;
    }
    
    public String getName() {
        return name;
    }
    
    public Collection<String> getReadingRoles() {
        return read.getRoles();
    }

    public Collection<String> getWritingRoles() {
        return write.getRoles();
    }
    
    public void addWritingRoles(String role){
        write.addRole(role);
    }
    
    public void addReadingRoles(String role){
        read.addRole(role);
    }
    
    @Override
    public String toString(){
        return this.getName() + "{read:" + this.getReadingRoles() + ", write:" + this.getWritingRoles() + "}";
    }
    
}
