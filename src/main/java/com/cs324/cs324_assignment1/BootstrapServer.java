package com.cs324.cs324_assignment1;

import com.distrilab.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class BootstrapServer {
    private static final int PORT = 1099;
    private final Server server;

    // Thread-safe map storing registered workers using their String ID as key
    private final ConcurrentHashMap<String, WorkerMetadata> activeWorkers = new ConcurrentHashMap<>();

    public BootstrapServer(int port) {
        this.server = ServerBuilder.forPort(port)
                .addService(new BootstrapServiceImpl())
                .build();
    }

    public void start() throws IOException {
        server.start();
        System.out.println("gRPC Bootstrap Node running on port " + PORT);

        // Ensures server closes cleanly if process is stopped
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gRPC Bootstrap server...");
            BootstrapServer.this.stop();
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BootstrapServer server = new BootstrapServer(PORT);
        server.start();
        server.blockUntilShutdown();
    }

    // gRPC Service implementation replacing RMI BootstrapInterface
    private class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {

        @Override
        public void registerWorker(WorkerMetadata request, StreamObserver<RegisterResponse> responseObserver) {
            activeWorkers.put(request.getWorkerId(), request);
            System.out.println("Worker " + request.getWorkerId() + " registered at " 
                    + request.getIpAddress() + ":" + request.getPort());

            RegisterResponse response = RegisterResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Worker registered successfully")
                    .addAllActiveWorkers(activeWorkers.values())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getActiveWorkers(Empty request, StreamObserver<WorkerList> responseObserver) {
            WorkerList list = WorkerList.newBuilder()
                    .addAllWorkers(activeWorkers.values())
                    .build();

            responseObserver.onNext(list);
            responseObserver.onCompleted();
        }

        @Override
        public void unregisterWorker(WorkerMetadata request, StreamObserver<UnregisterResponse> responseObserver) {
            activeWorkers.remove(request.getWorkerId());
            System.out.println("Worker " + request.getWorkerId() + " unregistered from Bootstrap Node.");

            UnregisterResponse response = UnregisterResponse.newBuilder()
                    .setSuccess(true)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}