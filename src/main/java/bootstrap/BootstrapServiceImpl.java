package bootstrap;

import com.distrilab.grpc.Ack;
import com.distrilab.grpc.BootstrapServiceGrpc;
import com.distrilab.grpc.CoordinatorInfo;
import com.distrilab.grpc.CoordinatorReport;
import com.distrilab.grpc.Empty;
import com.distrilab.grpc.HeartbeatRequest;
import com.distrilab.grpc.HeartbeatResponse;
import com.distrilab.grpc.MembershipEvent;
import com.distrilab.grpc.RegisterRequest;
import com.distrilab.grpc.RegisterResponse;
import com.distrilab.grpc.WorkerId;
import com.distrilab.grpc.WorkerInfo;
import com.distrilab.grpc.WorkerList;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * gRPC endpoints of the Bootstrap Node (replaces the RMI BootstrapInterface).
 * Validates requests and delegates to the {@link WorkerRegistry}.
 */
final class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {

    private final WorkerRegistry registry;

    BootstrapServiceImpl(WorkerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void registerWorker(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        WorkerInfo worker = request.getWorker();

        if (worker.getWorkerId() <= 0) {
            invalid(responseObserver, "worker_id must be a positive number");
            return;
        }
        if (worker.getHost().isBlank()) {
            invalid(responseObserver, "host must not be empty");
            return;
        }
        if (worker.getPort() <= 0 || worker.getPort() > 65535) {
            invalid(responseObserver, "port must be between 1 and 65535");
            return;
        }

        // Only the address is taken from the request; status fields are tracked by the registry.
        WorkerInfo address = WorkerInfo.newBuilder()
                .setWorkerId(worker.getWorkerId())
                .setHost(worker.getHost())
                .setPort(worker.getPort())
                .build();

        reply(responseObserver, registry.register(address));
    }

    @Override
    public void unregisterWorker(WorkerId request, StreamObserver<Ack> responseObserver) {
        boolean removed = registry.unregister(request.getWorkerId());

        reply(responseObserver, Ack.newBuilder()
                .setSuccess(removed)
                .setMessage(removed
                        ? "Worker " + request.getWorkerId() + " unregistered"
                        : "Worker " + request.getWorkerId() + " was not registered")
                .build());
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        reply(responseObserver, registry.heartbeat(request));
    }

    @Override
    public void getActiveWorkers(Empty request, StreamObserver<WorkerList> responseObserver) {
        reply(responseObserver, WorkerList.newBuilder()
                .addAllWorkers(registry.activeWorkers())
                .setCoordinator(registry.coordinatorInfo())
                .build());
    }

    @Override
    public void getWorker(WorkerId request, StreamObserver<WorkerInfo> responseObserver) {
        WorkerInfo worker = registry.find(request.getWorkerId());

        if (worker == null) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Worker " + request.getWorkerId() + " is not active")
                    .asRuntimeException());
            return;
        }
        reply(responseObserver, worker);
    }

    @Override
    public void getCoordinator(Empty request, StreamObserver<CoordinatorInfo> responseObserver) {
        reply(responseObserver, registry.coordinatorInfo());
    }

    @Override
    public void reportCoordinator(CoordinatorReport request, StreamObserver<CoordinatorInfo> responseObserver) {
        try {
            reply(responseObserver, registry.reportCoordinator(request.getCoordinatorId(), request.getTerm()));
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void watchMembership(Empty request, StreamObserver<MembershipEvent> responseObserver) {
        // Stop sending events once the watcher disconnects.
        ((ServerCallStreamObserver<MembershipEvent>) responseObserver)
                .setOnCancelHandler(() -> registry.unwatch(responseObserver));

        registry.watch(responseObserver);
    }

    private static <T> void reply(StreamObserver<T> observer, T response) {
        observer.onNext(response);
        observer.onCompleted();
    }

    private static void invalid(StreamObserver<?> observer, String message) {
        observer.onError(Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException());
    }
}
