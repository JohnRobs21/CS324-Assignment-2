package bootstrap;

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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Connection to the Bootstrap Node for workers and clients.
 *
 * Worker usage:
 * <pre>
 *   BootstrapClient bootstrap = new BootstrapClient(BootstrapNode.DEFAULT_HOST, BootstrapNode.DEFAULT_PORT);
 *   RegisterResponse joined = bootstrap.register(workerId, "localhost", myGrpcPort);
 *   bootstrap.startHeartbeats(
 *           () -> HeartbeatRequest.newBuilder().setWorkerId(workerId).setJac(jac).build(),
 *           response -> { if (!response.getCoordinator().getPresent()) startElection(); });
 *   bootstrap.watchMembership(event -> ...);   // optional: react to FAILED / JOINED immediately
 * </pre>
 * Client usage: {@code new BootstrapClient(host, port).getCoordinator()}.
 *
 * Blocking calls use a deadline, so an unreachable Bootstrap Node raises
 * {@link StatusRuntimeException} instead of hanging.
 */
public final class BootstrapClient implements AutoCloseable {

    private static final long CALL_TIMEOUT_MS = 2000;

    private final ManagedChannel channel;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub blockingStub;
    private final BootstrapServiceGrpc.BootstrapServiceStub asyncStub;
    private final ScheduledExecutorService heartbeatTimer;

    // Remembered by register() so heartbeats can re-register automatically.
    private volatile WorkerInfo self;
    private volatile long heartbeatIntervalMs = BootstrapNode.HEARTBEAT_INTERVAL_MS;

    public BootstrapClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = BootstrapServiceGrpc.newBlockingStub(channel);
        this.asyncStub = BootstrapServiceGrpc.newStub(channel);
        this.heartbeatTimer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "bootstrap-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Registers this worker. {@code host} and {@code port} are the address
     * of the worker's own gRPC server, which other workers will connect to.
     */
    public RegisterResponse register(int workerId, String host, int port) {
        WorkerInfo worker = WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost(host)
                .setPort(port)
                .build();

        RegisterResponse response = stub().registerWorker(
                RegisterRequest.newBuilder().setWorker(worker).build());

        if (response.getSuccess()) {
            self = worker;
            if (response.getHeartbeatIntervalMs() > 0) {
                heartbeatIntervalMs = response.getHeartbeatIntervalMs();
            }
        }
        return response;
    }

    /** Leaves the cluster gracefully and stops sending heartbeats. */
    public void unregister() {
        heartbeatTimer.shutdownNow();
        WorkerInfo worker = self;
        if (worker != null) {
            stub().unregisterWorker(WorkerId.newBuilder().setWorkerId(worker.getWorkerId()).build());
        }
    }

    /**
     * Sends a heartbeat every interval in the background. If the Bootstrap
     * Node no longer knows this worker (it was evicted, or the Bootstrap
     * Node restarted), the worker is registered again automatically.
     *
     * @param request  builds each heartbeat (worker ID, JAC, coordinator term)
     * @param listener receives each response, e.g. to notice that the
     *                 coordinator is gone; may be null
     */
    public void startHeartbeats(Supplier<HeartbeatRequest> request, Consumer<HeartbeatResponse> listener) {
        heartbeatTimer.scheduleWithFixedDelay(() -> {
            try {
                HeartbeatResponse response = stub().heartbeat(request.get());

                if (!response.getRegistered() && self != null) {
                    WorkerInfo worker = self;
                    System.out.println("Worker " + worker.getWorkerId()
                            + " is not registered with the Bootstrap Node; registering again.");
                    register(worker.getWorkerId(), worker.getHost(), worker.getPort());
                }

                if (listener != null) {
                    listener.accept(response);
                }
            } catch (StatusRuntimeException e) {
                System.out.println("Bootstrap Node unreachable: " + e.getStatus().getCode());
            } catch (RuntimeException e) {
                // An exception would cancel the scheduled task, so log it and keep going.
                System.out.println("Heartbeat error: " + e);
            }
        }, 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** Sends heartbeats that only carry the worker ID. Requires {@link #register} first. */
    public void startHeartbeats() {
        WorkerInfo worker = self;
        if (worker == null) {
            throw new IllegalStateException("register() must succeed before heartbeats start");
        }
        startHeartbeats(() -> HeartbeatRequest.newBuilder().setWorkerId(worker.getWorkerId()).build(), null);
    }

    /** Active workers plus the current coordinator. */
    public WorkerList getActiveWorkers() {
        return stub().getActiveWorkers(Empty.getDefaultInstance());
    }

    /** @throws StatusRuntimeException with status NOT_FOUND if the worker is not active */
    public WorkerInfo getWorker(int workerId) {
        return stub().getWorker(WorkerId.newBuilder().setWorkerId(workerId).build());
    }

    public CoordinatorInfo getCoordinator() {
        return stub().getCoordinator(Empty.getDefaultInstance());
    }

    /**
     * Reports the winner of an election. Use a higher term for every new
     * election; a report with an older term is ignored and the returned
     * value shows the coordinator the Bootstrap Node actually holds.
     */
    public CoordinatorInfo reportCoordinator(int coordinatorId, long term) {
        return stub().reportCoordinator(CoordinatorReport.newBuilder()
                .setCoordinatorId(coordinatorId)
                .setTerm(term)
                .build());
    }

    /**
     * Streams membership changes (JOINED, LEFT, FAILED, COORDINATOR_CHANGED,
     * COORDINATOR_LOST) to the listener until this client is closed.
     */
    public void watchMembership(Consumer<MembershipEvent> listener) {
        asyncStub.watchMembership(Empty.getDefaultInstance(), new StreamObserver<>() {
            @Override
            public void onNext(MembershipEvent event) {
                listener.accept(event);
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("Membership stream closed: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("Membership stream ended by the Bootstrap Node.");
            }
        });
    }

    @Override
    public void close() {
        heartbeatTimer.shutdownNow();
        channel.shutdownNow();
    }

    private BootstrapServiceGrpc.BootstrapServiceBlockingStub stub() {
        // A deadline is fixed when the stub is created, so make a fresh one per call.
        return blockingStub.withDeadlineAfter(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}
