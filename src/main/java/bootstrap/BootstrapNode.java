package bootstrap;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Standalone Bootstrap Node.
 *
 * Keeps the list of active workers and the current coordinator. Workers
 * register on start-up and then send a heartbeat every
 * HEARTBEAT_INTERVAL_MS; a worker that is silent for longer than
 * HEARTBEAT_TIMEOUT_MS (three missed heartbeats) is treated as failed.
 *
 * Usage: java bootstrap.BootstrapNode [port]
 */
public final class BootstrapNode {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 50050;

    static final long HEARTBEAT_INTERVAL_MS = 2000;
    static final long HEARTBEAT_TIMEOUT_MS = 3 * HEARTBEAT_INTERVAL_MS;

    // How often the registry is scanned for timed-out workers.
    private static final long EVICTION_CHECK_MS = 1000;

    private final int port;
    private final WorkerRegistry registry;
    private final Server server;
    private final ScheduledExecutorService failureDetector;

    public BootstrapNode(int port) {
        this.port = port;
        this.registry = new WorkerRegistry(HEARTBEAT_INTERVAL_MS, HEARTBEAT_TIMEOUT_MS);
        this.server = ServerBuilder.forPort(port)
                .addService(new BootstrapServiceImpl(registry))
                .build();
        this.failureDetector = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "bootstrap-failure-detector");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() throws IOException {
        server.start();

        failureDetector.scheduleAtFixedRate(() -> {
            try {
                registry.evictExpired();
            } catch (RuntimeException e) {
                // An exception would cancel the scheduled task, so log it and keep checking.
                System.out.println("[Bootstrap] Failure check error: " + e);
            }
        }, EVICTION_CHECK_MS, EVICTION_CHECK_MS, TimeUnit.MILLISECONDS);

        System.out.println("[Bootstrap] gRPC Bootstrap Node running on port " + port
                + " (heartbeat every " + HEARTBEAT_INTERVAL_MS + " ms, timeout "
                + HEARTBEAT_TIMEOUT_MS + " ms)");

        // Close cleanly if the process is stopped.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[Bootstrap] Shutting down...");
            BootstrapNode.this.stop();
        }));
    }

    public void stop() {
        failureDetector.shutdownNow();
        registry.closeWatchers();
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = DEFAULT_PORT;

        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("Port must be an integer. Usage: BootstrapNode [port]");
                return;
            }
        }

        BootstrapNode node = new BootstrapNode(port);
        node.start();
        node.blockUntilShutdown();
    }
}
