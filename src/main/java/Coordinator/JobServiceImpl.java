/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

package Coordinator;

import com.cs324.distrilab.grpc.JobServiceGrpc;
import com.cs324.distrilab.grpc.JobUpdate;
import com.cs324.distrilab.grpc.MaxJobRequest;
import com.cs324.distrilab.grpc.WorkerProgress;
import com.cs324.distrilab.grpc.PrimeSumJobRequest;
import com.cs324.distrilab.grpc.PrimeCountJobRequest;
import com.cs324.distrilab.grpc.TaskChunkRequest;
import com.cs324.distrilab.grpc.TaskChunkResponse;
import com.cs324.distrilab.grpc.TaskProgressUpdate;
import com.cs324.distrilab.grpc.WorkerServiceGrpc;
import com.cs324.distrilab.grpc.BootstrapServiceGrpc;
import com.cs324.distrilab.grpc.GetActiveWorkersRequest;
import com.cs324.distrilab.grpc.GetActiveWorkersResponse;
import com.cs324.distrilab.grpc.WorkerInfo;
import com.cs324.distrilab.grpc.RecordJobAssignmentRequest;
import com.cs324.distrilab.grpc.RecordJobAssignmentResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.TimeUnit;

public class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {

private java.util.List<WorkerInfo> getRegisteredWorkers() {

    ManagedChannel bootstrapChannel = ManagedChannelBuilder
            .forAddress("localhost", 50050)
            .usePlaintext()
            .build();

    try {
        var bootstrapStub =
                BootstrapServiceGrpc.newBlockingStub(bootstrapChannel)
                        .withDeadlineAfter(
                                3, java.util.concurrent.TimeUnit.SECONDS);

        // Discover workers that Bootstrap currently considers active.
        GetActiveWorkersResponse workersResponse =
                bootstrapStub.getActiveWorkers(
                        GetActiveWorkersRequest.newBuilder().build());

        java.util.List<WorkerInfo> activeWorkers =
                new java.util.ArrayList<>();

        for (WorkerInfo worker : workersResponse.getWorkersList()) {
            if (worker.getActive()) {
                activeWorkers.add(worker);
            }
        }

        if (activeWorkers.isEmpty()) {
            throw new IllegalStateException(
                    "No active workers are registered with Bootstrap Server.");
        }

        // Retrieve the currently elected coordinator.
        var coordinatorResponse =
                bootstrapStub.getCoordinator(
                        com.cs324.distrilab.grpc.GetCoordinatorRequest
                                .newBuilder()
                                .build());

        if (coordinatorResponse.getHasCoordinator()) {
            int leaderId = coordinatorResponse.getCoordinatorId();

            // Put the elected coordinator first in the worker list.
            activeWorkers.sort((workerA, workerB) -> {
                if (workerA.getWorkerId() == leaderId) {
                    return workerB.getWorkerId() == leaderId ? 0 : -1;
                }

                if (workerB.getWorkerId() == leaderId) {
                    return 1;
                }

                return Integer.compare(
                        workerA.getWorkerId(),
                        workerB.getWorkerId());
            });

            System.out.println(
                    "Job service discovered coordinator: Worker "
                    + leaderId);
        } else {
            System.out.println(
                    "No coordinator has been elected yet. "
                    + "Using the active-worker list.");
        }

        return activeWorkers;

    } finally {
        bootstrapChannel.shutdownNow();
    }
}

private void recordAssignment(int targetWorkerId) {

    ManagedChannel bootstrapChannel = ManagedChannelBuilder
            .forAddress("localhost", 50050)
            .usePlaintext()
            .build();

    try {
        var stub = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel)
                .withDeadlineAfter(3, TimeUnit.SECONDS);

        var coordinator = stub.getCoordinator(
                com.cs324.distrilab.grpc.GetCoordinatorRequest
                        .newBuilder()
                        .build());

        if (!coordinator.getHasCoordinator()) {
            throw new IllegalStateException(
                    "No coordinator is currently elected.");
        }

        RecordJobAssignmentRequest assignmentRequest =
                RecordJobAssignmentRequest.newBuilder()
                        .setCoordinatorId(coordinator.getCoordinatorId())
                        .setTargetWorkerId(targetWorkerId)
                        .build();

        RecordJobAssignmentResponse response =
                stub.recordJobAssignment(assignmentRequest);

        if (!response.getSuccess()) {
            throw new IllegalStateException(
                    "Assignment was rejected: " + response.getMessage());
        }

        System.out.println(
                "Assignment recorded for Worker " + targetWorkerId
                + ". Coordinator JAC: " + response.getUpdatedJac());

    } finally {
        bootstrapChannel.shutdownNow();
    }
}

private TaskChunkResponse executeWithRetry(
        TaskChunkRequest task,
        java.util.List<WorkerInfo> workers,
        int preferredWorkerId) {

    Exception lastError = null;

    // Try the worker originally assigned to this task first.
    for (WorkerInfo worker : workers) {
        if (worker.getWorkerId() == preferredWorkerId) {
            try {
                recordAssignment(worker.getWorkerId());
                return callWorker(worker, task);
            } catch (Exception e) {
                lastError = e;
                System.out.println(
                        "Worker " + worker.getWorkerId()
                        + " failed. Trying another worker...");
            }
            break;
        }
    }

    // Retry the unfinished task on another active worker.
    for (WorkerInfo worker : workers) {
        if (worker.getWorkerId() == preferredWorkerId) {
            continue;
        }

        try {
            System.out.println(
                    "Reassigning task " + task.getTaskId()
                    + " to Worker " + worker.getWorkerId());

            recordAssignment(worker.getWorkerId());
            return callWorker(worker, task);

        } catch (Exception e) {
            lastError = e;
            System.out.println(
                    "Worker " + worker.getWorkerId()
                    + " also failed: " + e.getMessage());
        }
    }

    throw new RuntimeException(
            "Task " + task.getTaskId()
            + " failed on all available workers.",
            lastError);
}

private TaskProgressUpdate executeWithProgressRetry(
        TaskChunkRequest task,
        java.util.List<WorkerInfo> workers,
        int preferredWorkerId,
        java.util.function.Consumer<TaskProgressUpdate> progressConsumer) {

    Exception lastError = null;

    // Try the originally assigned worker first.
    for (WorkerInfo worker : workers) {
        if (worker.getWorkerId() != preferredWorkerId) {
            continue;
        }

        try {
            recordAssignment(worker.getWorkerId());
            return callWorkerWithProgressBlocking(
                    worker, task, progressConsumer);
        } catch (Exception e) {
            lastError = e;
            System.out.println(
                    "Worker " + worker.getWorkerId()
                    + " failed during streaming. Trying another worker...");
        }

        break;
    }

    // Retry the task on another available worker.
    for (WorkerInfo worker : workers) {
        if (worker.getWorkerId() == preferredWorkerId) {
            continue;
        }

        try {
            System.out.println(
                    "Reassigning task " + task.getTaskId()
                    + " to Worker " + worker.getWorkerId());

            recordAssignment(worker.getWorkerId());
            return callWorkerWithProgressBlocking(
                    worker, task, progressConsumer);

        } catch (Exception e) {
            lastError = e;
            System.out.println(
                    "Worker " + worker.getWorkerId()
                    + " also failed: " + e.getMessage());
        }
    }

    throw new RuntimeException(
            "Task " + task.getTaskId()
            + " failed on all available workers.",
            lastError);
}

private TaskChunkResponse callWorker(
        WorkerInfo worker,
        TaskChunkRequest task) {

    ManagedChannel workerChannel = ManagedChannelBuilder
            .forAddress(worker.getHost(), worker.getPort())
            .usePlaintext()
            .build();

    try {
        return WorkerServiceGrpc.newBlockingStub(workerChannel)
                .withDeadlineAfter(
                        120, java.util.concurrent.TimeUnit.SECONDS)
                .executeTask(task);

    } finally {
        workerChannel.shutdown();
    }
}

private void callWorkerWithProgress(
        WorkerInfo worker,
        TaskChunkRequest task,
        java.util.function.Consumer<TaskProgressUpdate> progressConsumer,
        java.util.function.Consumer<Throwable> errorConsumer) {

    ManagedChannel workerChannel = ManagedChannelBuilder
            .forAddress(worker.getHost(), worker.getPort())
            .usePlaintext()
            .build();

    try {
        WorkerServiceGrpc.newStub(workerChannel)
                .withDeadlineAfter(120, TimeUnit.SECONDS)
                .executeTaskWithProgress(
                        task,
                        new StreamObserver<TaskProgressUpdate>() {
                            @Override
                            public void onNext(TaskProgressUpdate update) {
                                progressConsumer.accept(update);
                            }

                            @Override
                            public void onError(Throwable error) {
                                try {
                                    errorConsumer.accept(error);
                                } finally {
                                    workerChannel.shutdown();
                                }
                            }

                            @Override
                            public void onCompleted() {
                                workerChannel.shutdown();
                            }
                        });

    } catch (Exception e) {
        workerChannel.shutdownNow();
        errorConsumer.accept(e);
    }
}


private TaskProgressUpdate callWorkerWithProgressBlocking(
        WorkerInfo worker,
        TaskChunkRequest task,
        java.util.function.Consumer<TaskProgressUpdate> progressConsumer) {

    ManagedChannel workerChannel = ManagedChannelBuilder
            .forAddress(worker.getHost(), worker.getPort())
            .usePlaintext()
            .build();

    try {
        java.util.Iterator<TaskProgressUpdate> updates =
                WorkerServiceGrpc.newBlockingStub(workerChannel)
                        .withDeadlineAfter(120, TimeUnit.SECONDS)
                        .executeTaskWithProgress(task);

        TaskProgressUpdate finalUpdate = null;

        // Receive progress updates as the worker calculates.
        while (updates.hasNext()) {
            TaskProgressUpdate update = updates.next();

            // Forward each update to the coordinator's progress handler.
            progressConsumer.accept(update);

            // Keep the final update for the actual result.
            if (update.getCompleted()) {
                finalUpdate = update;
            }
        }

        if (finalUpdate == null) {
            throw new RuntimeException(
                    "Worker " + worker.getWorkerId()
                    + " ended without a final result.");
        }

        if (!finalUpdate.getSuccess()) {
            throw new RuntimeException(finalUpdate.getMessage());
        }

        return finalUpdate;

    } finally {
        workerChannel.shutdown();
    }
}
  
@Override
public void submitMax(
        MaxJobRequest request,
        StreamObserver<JobUpdate> responseObserver) {
    try {
        java.util.List<Integer> numbers = request.getNumbersList();
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("MAX requires at least one number.");
        }

        java.util.List<WorkerInfo> workers = getRegisteredWorkers();
        int taskCount = Math.min(workers.size(), numbers.size());
        java.util.List<TaskChunkRequest> tasks = new java.util.ArrayList<>();

        int baseSize = numbers.size() / taskCount;
        int remainder = numbers.size() % taskCount;
        int offset = 0;
        for (int i = 0; i < taskCount; i++) {
            int size = baseSize + (i < remainder ? 1 : 0);
            java.util.List<Integer> chunk =
                    new java.util.ArrayList<>(numbers.subList(offset, offset + size));
            tasks.add(TaskChunkRequest.newBuilder()
                    .setJobId(request.getJobId())
                    .setTaskId(request.getJobId() + "-task" + (i + 1))
                    .setJobType("MAX")
                    .addAllNumbers(chunk)
                    .build());
            offset += size;
        }

        executeDistributedTasks(request.getJobId(), "MAX", tasks, workers,
                numbers.size(), responseObserver, "Searching for maximum...");
    } catch (Exception e) {
        responseObserver.onError(io.grpc.Status.INTERNAL
                .withDescription("MAX job failed: " + e.getMessage())
                .withCause(e).asRuntimeException());
    }
}

@Override
public void submitPrimeSum(
        PrimeSumJobRequest request,
        StreamObserver<JobUpdate> responseObserver) {
    try {
        int start = request.getStart();
        int end = request.getEnd();
        if (start > end) {
            throw new IllegalArgumentException(
                    "Start must be less than or equal to end.");
        }

        long rangeSize = (long) end - (long) start + 1L;
        if (rangeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "PRIMESUM range is too large for this implementation.");
        }

        java.util.List<WorkerInfo> workers = getRegisteredWorkers();
        int taskCount = (int) Math.min((long) workers.size(), rangeSize);
        java.util.List<TaskChunkRequest> tasks = new java.util.ArrayList<>();
        long baseSize = rangeSize / taskCount;
        long remainder = rangeSize % taskCount;
        long nextStart = start;

        for (int i = 0; i < taskCount; i++) {
            long size = baseSize + (i < remainder ? 1L : 0L);
            long nextEnd = nextStart + size - 1L;
            tasks.add(TaskChunkRequest.newBuilder()
                    .setJobId(request.getJobId())
                    .setTaskId(request.getJobId() + "-task" + (i + 1))
                    .setJobType("PRIMESUM")
                    .setStart((int) nextStart)
                    .setEnd((int) nextEnd)
                    .build());
            nextStart = nextEnd + 1L;
        }

        executeDistributedTasks(request.getJobId(), "PRIMESUM", tasks, workers,
                (int) rangeSize, responseObserver, "Calculating prime sum...");
    } catch (Exception e) {
        responseObserver.onError(io.grpc.Status.INTERNAL
                .withDescription("PRIMESUM job failed: " + e.getMessage())
                .withCause(e).asRuntimeException());
    }
}

@Override
public void submitPrimeCount(
        PrimeCountJobRequest request,
        StreamObserver<JobUpdate> responseObserver) {
    try {
        java.util.List<Integer> numbers = request.getNumbersList();
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "PRIMECOUNT requires at least one number.");
        }

        java.util.List<WorkerInfo> workers = getRegisteredWorkers();
        int taskCount = Math.min(workers.size(), numbers.size());
        int baseSize = numbers.size() / taskCount;
        int remainder = numbers.size() % taskCount;
        int offset = 0;
        java.util.List<TaskChunkRequest> tasks = new java.util.ArrayList<>();

        for (int i = 0; i < taskCount; i++) {
            int size = baseSize + (i < remainder ? 1 : 0);
            java.util.List<Integer> chunk =
                    new java.util.ArrayList<>(numbers.subList(offset, offset + size));
            tasks.add(TaskChunkRequest.newBuilder()
                    .setJobId(request.getJobId())
                    .setTaskId(request.getJobId() + "-task" + (i + 1))
                    .setJobType("PRIMECOUNT")
                    .addAllNumbers(chunk)
                    .build());
            offset += size;
        }

        executeDistributedTasks(request.getJobId(), "PRIMECOUNT", tasks, workers,
                numbers.size(), responseObserver, "Counting prime numbers...");
    } catch (Exception e) {
        responseObserver.onError(io.grpc.Status.INTERNAL
                .withDescription("PRIMECOUNT job failed: " + e.getMessage())
                .withCause(e).asRuntimeException());
    }
}

/**
 * Executes one balanced task partition per available worker, streams aggregate
 * progress, retries failed partitions on another registered worker, and combines
 * the final worker results. MAX uses max-reduction; the other jobs use sum-reduction.
 */
private void executeDistributedTasks(
        String jobId,
        String jobType,
        java.util.List<TaskChunkRequest> tasks,
        java.util.List<WorkerInfo> workers,
        int totalUnits,
        StreamObserver<JobUpdate> responseObserver,
        String progressMessage) throws Exception {

    if (workers.isEmpty() || tasks.isEmpty()) {
        throw new IllegalStateException("No active workers are available.");
    }

    final int taskCount = tasks.size();
    final int[] taskTotals = new int[taskCount];
    final int[] completedUnits = new int[taskCount];
    final int[] actualWorkerIds = new int[taskCount];
    final Object progressLock = new Object();
    java.util.concurrent.ExecutorService executor =
            java.util.concurrent.Executors.newFixedThreadPool(taskCount);

    for (int i = 0; i < taskCount; i++) {
        TaskChunkRequest task = tasks.get(i);
        taskTotals[i] = "PRIMESUM".equals(jobType)
                ? (int) ((long) task.getEnd() - task.getStart() + 1L)
                : task.getNumbersCount();
        actualWorkerIds[i] = workers.get(i).getWorkerId();
    }

    try {
        java.util.List<java.util.concurrent.Future<TaskProgressUpdate>> futures =
                new java.util.ArrayList<>();

        for (int i = 0; i < taskCount; i++) {
            final int taskIndex = i;
            TaskChunkRequest task = tasks.get(i);
            int preferredWorkerId = workers.get(i).getWorkerId();

            futures.add(executor.submit(() -> executeWithProgressRetry(
                    task, workers, preferredWorkerId, progress -> {
                        synchronized (progressLock) {
                            completedUnits[taskIndex] = Math.max(0, Math.min(
                                    taskTotals[taskIndex],
                                    progress.getCompletedUnits()));
                            if (progress.getWorkerId() > 0) {
                                actualWorkerIds[taskIndex] = progress.getWorkerId();
                            }

                            int completed = 0;
                            for (int value : completedUnits) {
                                completed += value;
                            }

                            JobUpdate.Builder update = JobUpdate.newBuilder()
                                    .setJobId(jobId)
                                    .setOverallPercentage(
                                            totalUnits == 0 ? 100.0
                                                    : completed * 100.0 / totalUnits)
                                    .setCompleted(false)
                                    .setMessage(progressMessage);

                            for (int j = 0; j < taskCount; j++) {
                                double percentage = taskTotals[j] == 0 ? 100.0
                                        : completedUnits[j] * 100.0 / taskTotals[j];
                                update.addWorkerProgress(WorkerProgress.newBuilder()
                                        .setWorkerId(actualWorkerIds[j])
                                        .setCompletedUnits(completedUnits[j])
                                        .setTotalUnits(taskTotals[j])
                                        .setPercentage(percentage)
                                        .setStatus(completedUnits[j] >= taskTotals[j]
                                                ? "Completed" : "Processing")
                                        .build());
                            }
                            responseObserver.onNext(update.build());
                        }
                    })));
        }

        long combinedResult = "MAX".equals(jobType)
                ? Long.MIN_VALUE : 0L;
        for (java.util.concurrent.Future<TaskProgressUpdate> future : futures) {
            TaskProgressUpdate result = future.get();
            if (!result.getSuccess()) {
                throw new IllegalStateException(result.getMessage());
            }
            if ("MAX".equals(jobType)) {
                combinedResult = Math.max(combinedResult, result.getResult());
            } else {
                combinedResult += result.getResult();
            }
        }

        synchronized (progressLock) {
            JobUpdate.Builder finalUpdate = JobUpdate.newBuilder()
                    .setJobId(jobId)
                    .setOverallPercentage(100.0)
                    .setCompleted(true)
                    .setResult(combinedResult)
                    .setMessage(jobType + " completed using " + taskCount
                            + " worker task(s)");

            for (int i = 0; i < taskCount; i++) {
                completedUnits[i] = taskTotals[i];
                finalUpdate.addWorkerProgress(WorkerProgress.newBuilder()
                        .setWorkerId(actualWorkerIds[i])
                        .setCompletedUnits(taskTotals[i])
                        .setTotalUnits(taskTotals[i])
                        .setPercentage(100.0)
                        .setStatus("Completed")
                        .build());
            }
            responseObserver.onNext(finalUpdate.build());
        }

        notifyCompletedJob(jobId);
        responseObserver.onCompleted();
    } finally {
        executor.shutdownNow();
    }
}

  private void notifyCompletedJob(String jobId) {
    ManagedChannel bootstrapChannel = ManagedChannelBuilder
            .forAddress("localhost", 50050)
            .usePlaintext()
            .build();

    try {
        var coordinator = BootstrapServiceGrpc
                .newBlockingStub(bootstrapChannel)
                .withDeadlineAfter(3, TimeUnit.SECONDS)
                .getCoordinator(
                        com.cs324.distrilab.grpc.GetCoordinatorRequest
                                .newBuilder()
                                .build());

        if (!coordinator.getHasCoordinator()) {
            System.out.println(
                    "No coordinator available to record completed job.");
            return;
        }

        java.util.List<WorkerInfo> workers = getRegisteredWorkers();

        for (WorkerInfo worker : workers) {
            if (worker.getWorkerId() == coordinator.getCoordinatorId()) {
                ManagedChannel workerChannel = ManagedChannelBuilder
                        .forAddress(worker.getHost(), worker.getPort())
                        .usePlaintext()
                        .build();

                try {
                    var response = WorkerServiceGrpc
                            .newBlockingStub(workerChannel)
                            .withDeadlineAfter(3, TimeUnit.SECONDS)
                            .recordCompletedJob(
                                    com.cs324.distrilab.grpc.CompletedJobRequest
                                            .newBuilder()
                                            .setJobId(jobId)
                                            .build());

                    System.out.println(response.getMessage());
                } finally {
                    workerChannel.shutdownNow();
                }

                return;
            }
        }

        System.out.println(
                "Could not find the elected coordinator among active workers.");

    } catch (Exception e) {
        System.err.println(
                "Could not record completed job " + jobId + ": "
                + e.getMessage());
    } finally {
        bootstrapChannel.shutdownNow();
    }
}
}
