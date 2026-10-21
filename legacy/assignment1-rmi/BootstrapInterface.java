/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package Interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *
 * @author gound
 */
public interface BootstrapInterface extends Remote{
    
    void registerWorker(int workerId,WorkerInterface worker)
        throws RemoteException;
    
    WorkerInterface[] getActiveWorkers()
        throws RemoteException;
    
    void unregisterWorker(int workerId)
        throws RemoteException;
    
}
