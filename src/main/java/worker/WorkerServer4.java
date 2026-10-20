/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */


package worker;

import com.cs324.distrilab.grpc.BootstrapServiceGrpc;
import com.cs324.distrilab.grpc.RegisterWorkerRequest;
import com.cs324.distrilab.grpc.RegisterWorkerResponse;
import com.cs324.distrilab.grpc.WorkerHeartbeat;
import com.cs324.distrilab.grpc.WorkerInfo;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WorkerServer4 {

    public static void main(String[] args) {
        int workerId = 4;
        int port = 50055;

        try {
            // Step 1: Connect to the Bootstrap Server.
            ManagedChannel bootstrapChannel = ManagedChannelBuilder
                    .forAddress("localhost", 50050)
                    .usePlaintext()
                    .build();

            try {
                // Step 2: Register Worker 4.
                WorkerInfo workerInfo = WorkerInfo.newBuilder()
                        .setWorkerId(workerId)
                        .setJac(workerId)
                        .setHost("localhost")
                        .setPort(port)
                        .setActive(true)
                        .build();

                RegisterWorkerRequest request =
                        RegisterWorkerRequest.newBuilder()
                                .setWorker(workerInfo)
                                .build();

                RegisterWorkerResponse response =
                        BootstrapServiceGrpc.newBlockingStub(bootstrapChannel)
                                .registerWorker(request);

                if (!response.getSuccess()) {
                    throw new IllegalStateException(
                            "Worker registration failed: "
                                    + response.getMessage());
                }

                System.out.println(
                        "Worker " + workerId
                                + " registered with Bootstrap Server.");

            } finally {
                bootstrapChannel.shutdown();
            }

            // Step 3: Send heartbeats every 3 seconds.
            ScheduledExecutorService heartbeatScheduler =
                    Executors.newSingleThreadScheduledExecutor();

            heartbeatScheduler.scheduleAtFixedRate(() -> {
                ManagedChannel heartbeatChannel = ManagedChannelBuilder
                        .forAddress("localhost", 50050)
                        .usePlaintext()
                        .build();

                try {
                    WorkerHeartbeat heartbeat =
                            WorkerHeartbeat.newBuilder()
                                    .setWorkerId(workerId)
                                    .build();

                    BootstrapServiceGrpc.newBlockingStub(heartbeatChannel)
                            .withDeadlineAfter(2, TimeUnit.SECONDS)
                            .heartbeat(heartbeat);

                } catch (Exception e) {
                    System.err.println(
                            "Worker " + workerId
                                    + " could not contact Bootstrap.");

                } finally {
                    heartbeatChannel.shutdown();
                }
            }, 0, 3, TimeUnit.SECONDS);

            // Step 4: Start Worker 4's gRPC server.
            Server server = ServerBuilder.forPort(port)
                    .addService(new WorkerImpl(workerId))
                    .build()
                    .start();

            System.out.println(
                    "gRPC Worker " + workerId
                            + " started on port " + port);

            // Step 5: Shut down cleanly.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Stopping Worker 4");
                heartbeatScheduler.shutdownNow();
                server.shutdown();
            }));

            server.awaitTermination();

        } catch (Exception e) {
            System.err.println("Could not start Worker 4.");
            e.printStackTrace();
        }
    }
}

