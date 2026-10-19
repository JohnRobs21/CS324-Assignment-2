package bootstrap;

import com.distrilab.grpc.CoordinatorInfo;
import com.distrilab.grpc.HeartbeatRequest;
import com.distrilab.grpc.HeartbeatResponse;
import com.distrilab.grpc.MembershipEvent;
import com.distrilab.grpc.RegisterResponse;
import com.distrilab.grpc.WorkerInfo;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Cluster membership held by the Bootstrap Node.
 *
 * A worker stays active only while it keeps sending heartbeats. A worker
 * whose last heartbeat is older than the timeout is evicted and reported
 * as FAILED to every watcher; if it was the coordinator, COORDINATOR_LOST
 * is reported too so the workers can start a new election.
 *
 * All methods are synchronized, so every watcher sees membership changes
 * in the same order they were applied.
 */
final class WorkerRegistry {

    private static final int NO_COORDINATOR = -1;

    private static final class Entry {
        final WorkerInfo address;
        int jac;
        long lastHeartbeatNanos;

        Entry(WorkerInfo address, long now) {
            this.address = address;
            this.lastHeartbeatNanos = now;
        }
    }

    private final long heartbeatIntervalMillis;
    private final long heartbeatTimeoutNanos;

    // Sorted by worker ID so lookups list workers in a stable order.
    private final Map<Integer, Entry> workers = new TreeMap<>();
    private final List<StreamObserver<MembershipEvent>> watchers = new ArrayList<>();

    private int coordinatorId = NO_COORDINATOR;
    private long coordinatorTerm = 0;

    WorkerRegistry(long heartbeatIntervalMillis, long heartbeatTimeoutMillis) {
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.heartbeatTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(heartbeatTimeoutMillis);
    }

    /**
     * Registers a worker. A worker that restarts with the same ID and
     * address simply replaces its old entry; a different live worker
     * already using the ID is rejected.
     */
    synchronized RegisterResponse register(WorkerInfo address) {
        long now = System.nanoTime();
        int id = address.getWorkerId();
        Entry existing = workers.get(id);

        if (existing != null && !sameAddress(existing.address, address) && !isExpired(existing, now)) {
            log("Rejected Worker " + id + " at " + describe(address)
                    + ": ID already used by " + describe(existing.address));
            return RegisterResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Worker ID " + id + " is already registered at " + describe(existing.address))
                    .build();
        }

        Entry entry = new Entry(address, now);
        workers.put(id, entry);

        if (existing == null || !sameAddress(existing.address, address)) {
            log("Worker " + id + " registered at " + describe(address)
                    + " (" + workers.size() + " active)");
            broadcast(MembershipEvent.Type.JOINED, toInfo(entry, now));
        } else {
            log("Worker " + id + " re-registered at " + describe(address));
        }

        return RegisterResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Worker " + id + " registered")
                .addAllActiveWorkers(activeWorkers())
                .setCoordinator(coordinatorInfo())
                .setHeartbeatIntervalMs(heartbeatIntervalMillis)
                .build();
    }

    /** Removes a worker that is leaving gracefully. Returns false if it was not registered. */
    synchronized boolean unregister(int workerId) {
        Entry removed = workers.remove(workerId);
        if (removed == null) {
            return false;
        }
        log("Worker " + workerId + " left (" + workers.size() + " active)");
        removeWorker(removed, MembershipEvent.Type.LEFT);
        return true;
    }

    synchronized HeartbeatResponse heartbeat(HeartbeatRequest request) {
        Entry entry = workers.get(request.getWorkerId());
        if (entry == null) {
            // Evicted earlier or the Bootstrap Node restarted: the worker must register again.
            return HeartbeatResponse.newBuilder()
                    .setRegistered(false)
                    .setCoordinator(coordinatorInfo())
                    .setActiveWorkerCount(workers.size())
                    .build();
        }

        entry.lastHeartbeatNanos = System.nanoTime();
        entry.jac = request.getJac();

        // Lets a restarted Bootstrap Node relearn the coordinator. Only a
        // newer term, or the current term when none is known, is accepted.
        if (request.getIsCoordinator()) {
            long term = request.getCoordinatorTerm();
            boolean newer = term > coordinatorTerm;
            boolean fillsGap = term == coordinatorTerm && coordinatorId == NO_COORDINATOR;
            if ((newer || fillsGap) && coordinatorId != request.getWorkerId()) {
                setCoordinator(request.getWorkerId(), term);
            }
        }

        return HeartbeatResponse.newBuilder()
                .setRegistered(true)
                .setCoordinator(coordinatorInfo())
                .setActiveWorkerCount(workers.size())
                .build();
    }

    /**
     * Records an elected coordinator. The report is accepted when its term
     * is newer than the current one, or equal to it while no other
     * coordinator holds that term. Returns the coordinator now held.
     *
     * @throws IllegalArgumentException if the worker is not registered
     */
    synchronized CoordinatorInfo reportCoordinator(int workerId, long term) {
        if (!workers.containsKey(workerId)) {
            throw new IllegalArgumentException("Worker " + workerId + " is not registered");
        }

        boolean newer = term > coordinatorTerm;
        boolean sameTerm = term == coordinatorTerm
                && (coordinatorId == NO_COORDINATOR || coordinatorId == workerId);

        if (newer || sameTerm) {
            if (coordinatorId != workerId || coordinatorTerm != term) {
                setCoordinator(workerId, term);
            }
        } else {
            log("Ignored stale coordinator report: Worker " + workerId + " term " + term
                    + " (current: Worker " + coordinatorId + " term " + coordinatorTerm + ")");
        }
        return coordinatorInfo();
    }

    /** Evicts every worker whose heartbeat has timed out. Called periodically by the Bootstrap Node. */
    synchronized void evictExpired() {
        long now = System.nanoTime();
        Iterator<Entry> it = workers.values().iterator();
        List<Entry> expired = new ArrayList<>();

        while (it.hasNext()) {
            Entry entry = it.next();
            if (isExpired(entry, now)) {
                it.remove();
                expired.add(entry);
            }
        }

        for (Entry entry : expired) {
            log("Worker " + entry.address.getWorkerId() + " FAILED: no heartbeat for "
                    + TimeUnit.NANOSECONDS.toMillis(now - entry.lastHeartbeatNanos) + " ms ("
                    + workers.size() + " active)");
            removeWorker(entry, MembershipEvent.Type.FAILED);
        }
    }

    synchronized List<WorkerInfo> activeWorkers() {
        long now = System.nanoTime();
        List<WorkerInfo> result = new ArrayList<>(workers.size());
        for (Entry entry : workers.values()) {
            result.add(toInfo(entry, now));
        }
        return result;
    }

    /** Returns the worker with the given ID, or null if it is not active. */
    synchronized WorkerInfo find(int workerId) {
        Entry entry = workers.get(workerId);
        return entry == null ? null : toInfo(entry, System.nanoTime());
    }

    synchronized CoordinatorInfo coordinatorInfo() {
        Entry entry = workers.get(coordinatorId);
        CoordinatorInfo.Builder info = CoordinatorInfo.newBuilder().setTerm(coordinatorTerm);
        if (entry != null) {
            info.setPresent(true).setCoordinator(toInfo(entry, System.nanoTime()));
        }
        return info.build();
    }

    /** Subscribes a watcher: sends the current membership, then every later change. */
    synchronized void watch(StreamObserver<MembershipEvent> watcher) {
        long now = System.nanoTime();
        try {
            for (Entry entry : workers.values()) {
                watcher.onNext(event(MembershipEvent.Type.JOINED, toInfo(entry, now)));
            }
            CoordinatorInfo coordinator = coordinatorInfo();
            if (coordinator.getPresent()) {
                watcher.onNext(event(MembershipEvent.Type.COORDINATOR_CHANGED, coordinator.getCoordinator()));
            }
            watchers.add(watcher);
        } catch (RuntimeException e) {
            // The watcher disconnected while the snapshot was being sent.
        }
    }

    synchronized void unwatch(StreamObserver<MembershipEvent> watcher) {
        watchers.remove(watcher);
    }

    /** Ends every membership stream; used when the Bootstrap Node shuts down. */
    synchronized void closeWatchers() {
        for (StreamObserver<MembershipEvent> watcher : watchers) {
            try {
                watcher.onCompleted();
            } catch (RuntimeException ignored) {
                // Already disconnected.
            }
        }
        watchers.clear();
    }

    private void removeWorker(Entry removed, MembershipEvent.Type type) {
        WorkerInfo info = toInfo(removed, System.nanoTime());
        broadcast(type, info);

        if (removed.address.getWorkerId() == coordinatorId) {
            coordinatorId = NO_COORDINATOR;
            log("Coordinator Worker " + info.getWorkerId() + " is gone (term " + coordinatorTerm
                    + "); waiting for a new election");
            broadcast(MembershipEvent.Type.COORDINATOR_LOST, info);
        }
    }

    private void setCoordinator(int workerId, long term) {
        coordinatorId = workerId;
        coordinatorTerm = term;
        log("Coordinator is now Worker " + workerId + " (term " + term + ")");
        broadcast(MembershipEvent.Type.COORDINATOR_CHANGED, toInfo(workers.get(workerId), System.nanoTime()));
    }

    private void broadcast(MembershipEvent.Type type, WorkerInfo worker) {
        MembershipEvent event = event(type, worker);
        Iterator<StreamObserver<MembershipEvent>> it = watchers.iterator();
        while (it.hasNext()) {
            try {
                it.next().onNext(event);
            } catch (RuntimeException e) {
                // The watcher disconnected; stop sending to it.
                it.remove();
            }
        }
    }

    private MembershipEvent event(MembershipEvent.Type type, WorkerInfo worker) {
        return MembershipEvent.newBuilder()
                .setType(type)
                .setWorker(worker)
                .setCoordinator(coordinatorInfo())
                .setTimestampMs(System.currentTimeMillis())
                .build();
    }

    private boolean isExpired(Entry entry, long now) {
        return now - entry.lastHeartbeatNanos > heartbeatTimeoutNanos;
    }

    private static WorkerInfo toInfo(Entry entry, long now) {
        return entry.address.toBuilder()
                .setJac(entry.jac)
                .setMillisSinceHeartbeat(TimeUnit.NANOSECONDS.toMillis(now - entry.lastHeartbeatNanos))
                .build();
    }

    private static boolean sameAddress(WorkerInfo a, WorkerInfo b) {
        return a.getHost().equals(b.getHost()) && a.getPort() == b.getPort();
    }

    private static String describe(WorkerInfo worker) {
        return worker.getHost() + ":" + worker.getPort();
    }

    private static void log(String message) {
        System.out.println("[Bootstrap] " + message);
    }
}
