/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */


package Coordinator;

import com.cs324.distrilab.grpc.*;
import io.grpc.stub.StreamObserver;

public class CoordinatorJobServiceImpl
        extends CoordinatorJobServiceGrpc.CoordinatorJobServiceImplBase {

    private final JobServiceImpl jobService = new JobServiceImpl();

    @Override
    public void submitMax( MaxJobRequest request, StreamObserver
            <JobUpdate> observer) {
        jobService.submitMax(request, observer);
    }

    @Override
    public void submitPrimeSum (PrimeSumJobRequest request, StreamObserver 
            <JobUpdate> observer) {
        jobService.submitPrimeSum(request, observer);
    }

    @Override
    public void submitPrimeCount(PrimeCountJobRequest request,StreamObserver
            <JobUpdate> observer) {
        jobService.submitPrimeCount(request, observer);
    }
}

