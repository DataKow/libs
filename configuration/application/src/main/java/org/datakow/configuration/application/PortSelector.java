package org.datakow.configuration.application;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

/**
 * Util class to find the next available port between a min and max
 * 
 * @author kevin.off
 */
public class PortSelector{

    /**
     * Returns the next available port between min and max.
     * 
     * @param minPort The minimum port
     * @param maxPort The maximum port
     * @return The next available port or exception
     */
    public static int getNextAvailablePort(int minPort, int maxPort){
        boolean found = false;
        int availablePort;
        for (availablePort = minPort; availablePort <= maxPort; availablePort++){
            if (available(availablePort)){
                found = true;
                break;
            }
        }
        if (found){
            return availablePort;
        }else{
            throw new RuntimeException("Cannot find an open port between " + minPort + " and " + maxPort + ".");
        }
    }
    
    /**
     * Determines if a port is available by creating and binding to a socket.
     * 
     * @param port The port to check
     * @return True if the port is open.
     */
    public static boolean available(int port) {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    } 
}
