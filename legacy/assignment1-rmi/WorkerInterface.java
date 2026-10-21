/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package Interface;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**remote interface used for communication between worker nodes.
 * remotely using java rmi
 */

/**
 *
 * @author gound
 */
public interface WorkerInterface extends Remote {
     
     // Returns the ID of the worker.
    int getWorkerId() throws RemoteException;

    
     //Sends a test message to another worker.
    // message received from another worker
    
    int getJac() throws RemoteException;
    
    // Returns the current Job Allocation Counter (JAC).
 
    void receiveMessage(String message) throws RemoteException;
    
    int processMaxJob(int[] numbers) throws RemoteException;
    
    int processPrimeSumJob(int start, int end) throws RemoteException;
    
    int processPrimeCountJob(int[] numbers) throws RemoteException;
    
    
    // Receives an ELECTION message from another worker.
    // unique ID of the election
    void receiveElection(String electionId) throws RemoteException;
   
   /**
    * Receives an ELECTION message together with
    * the worker's ID and JAC.
    *
    *  electionId unique ID of the election
    *  candidateId ID of the worker
    *  candidateJac JAC value of the worker
    */
    void receiveElectionData(String electionId,int candidateId,int candidateJac)    
        throws RemoteException;
    
    void finishElection(String electionId)
            throws RemoteException;
    
    
    // Receives a COORDINATOR message.
    // coordinatorId ID of the elected coordinator
    
    void receiveCoordinator(int coordinatorId)
        throws RemoteException;

   
    // Adds another worker as a direct neighbour.
    // neighbourId ID of the neighbour worker
    // neighbour remote reference to the neighbour
   
    void addNeighbour(int neighbourId,WorkerInterface neighbour)       
        throws RemoteException;
    
    // Client job submission methods
    int submitMaxJob(int[] numbers) throws RemoteException;

    int submitPrimeSumJob(int start, int end) throws RemoteException;

    int submitPrimeCountJob(int[] numbers) throws RemoteException;

    // Returns the current coordinator ID
    int getCoordinatorId() throws RemoteException;
}
