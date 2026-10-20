# CS324 Assignment 2 --- Distributed Processing System

## Overview

This project implements a distributed processing system in Java using
**gRPC** and **Protocol Buffers**. A client submits a numerical job, the
coordinator distributes work to registered workers, and the client
receives the result and worker progress information.

Supported operations: - **MAX** --- finds the maximum value in a list of
numbers. - **PRIMESUM(start, end)** --- calculates the sum of prime
numbers in the inclusive range. - **PRIMECOUNT(numbers)** --- counts the
prime numbers in the supplied list.

## Technology

-   Java 22
-   Maven
-   gRPC
-   Protocol Buffers
-   NetBeans IDE
-   Client GUI

## System Architecture

The Bootstrap Server maintains worker registration and coordinator
information. The Coordinator Server accepts client jobs and distributes
tasks to available workers. Workers execute assigned tasks and return
results and progress information through gRPC.

  Component               Port
  -------------------- -------
  Bootstrap Server       50050
  Coordinator Server     50051
  Worker 1               50052
  Worker 2               50053
  Worker 3               50054
  Worker 4               50055

Workers register with the Bootstrap Server and send periodic heartbeats.
Distributed communication uses gRPC and Protocol Buffers; a Java RMI
registry is not required.

## Prerequisites

-   JDK 22 (or the JDK version configured for the project)
-   Apache Maven
-   NetBeans IDE or another Maven-compatible Java IDE

## Build the Project

1.  Open `CS324_Assignment2` in NetBeans.
2.  Check that the project uses the installed JDK.
3.  Allow Maven to download dependencies and generate gRPC classes from
    the `.proto` definitions.
4.  Select **Clean and Build** and confirm that the build succeeds.

From a terminal, change to your project directory and run:

``` powershell
cd "C:\Users\gound\OneDrive\Documents\NetBeansProjects\CS324_Assignment2"
mvn clean package
```

The path above is the original development path. Replace it if your
project is stored elsewhere.

## Running the System

Start each component in a separate NetBeans run session or terminal,
using the project's configured main classes:

1.  Start **BootstrapServer** on port `50050`.
2.  Start **CoordinatorServer** on port `50051`.
3.  Start Worker 1 on port `50052`.
4.  Start Worker 2 on port `50053`.
5.  Start Worker 3 on port `50054`.
6.  Start Worker 4 on port `50055`.
7.  Open the Client GUI and submit a job.

Start Bootstrap first so the coordinator and workers can register or
discover coordinator information. Keep the server processes running
while testing. If worker IDs or ports are passed as command-line
arguments, use the values configured for each worker class.

## Using the Client

1.  Launch the Client GUI.
2.  Select `MAX`, `PRIMESUM`, or `PRIMECOUNT`.
3.  Enter the required numbers or range.
4.  Submit the job.
5.  Review the result and per-worker progress information.

Example expected results: - `PRIMESUM(1, 100)` → **1060** -
`PRIMESUM(1, 1000)` → **76127**

## Features and Testing Notes

The following behaviours have been observed during development tests:

-   Worker registration with the Bootstrap Server.
-   Heartbeat monitoring and inactive-worker detection after heartbeat
    timeouts.
-   Coordinator election and re-election after a coordinator becomes
    inactive.
-   Work assignment logs showing all four worker IDs.
-   Correct results for the two `PRIMESUM` examples above.
-   Worker-level progress breakdown in the Client GUI.
-   Successful completion notifications in recent test logs.
-   A rejoining worker registering again and subsequently appearing in
    assignment logs.
-   A new job completing successfully after coordinator failover from
    Worker 1 to Worker 2.

**Testing limitations:** These observations do not prove that an
unfinished task is always reassigned and completed after a worker fails
during execution. The progress bar has also been observed to move very
quickly, so continuous visible progress updates have not yet been fully
verified. Run focused tests before describing these behaviours as fully
verified.

## Failure and Recovery Testing

### Worker failure

1.  Start the Bootstrap Server, Coordinator Server, and available
    workers.
2.  Submit a sufficiently large job.
3.  While it is running, stop a worker that is processing a task.
4.  Check whether the Bootstrap Server detects the heartbeat timeout.
5.  Check coordinator logs for reassignment of unfinished work.
6.  Verify that the final result is correct and the job completes.

### Coordinator failure

1.  Stop the current coordinator.
2.  Check whether another worker is elected.
3.  Submit a **new** job to verify that the system accepts work after
    failover.

A successful new job after failover does not prove recovery of a job
that was already in progress.

### Dynamic worker joining

1.  Start a worker while the system is running.
2.  Confirm that it registers with the Bootstrap Server.
3.  Submit or observe a job and verify that the rejoined worker receives
    work.

## Troubleshooting

-   **Maven build errors:** Check the configured JDK and Maven
    dependencies, then run Clean and Build again.
-   **Worker does not register:** Confirm that the Bootstrap Server is
    running on port `50050` and the worker uses the correct address and
    port.
-   **Client cannot connect:** Confirm that the Coordinator Server is
    running on port `50051`.
-   **Port already in use:** Stop the older process using that port
    before starting another instance.
-   **Worker marked inactive:** Check that its process is running and
    can send heartbeats to the Bootstrap Server.
-   **Progress bar completes too quickly:** This may be caused by a
    short job or progress updates being sent only at task boundaries.
    Verify the streaming implementation before treating gradual
    real-time updates as complete.

## Important Project Files

-   `pom.xml` --- Maven dependencies and build configuration.
-   `.proto` file(s) --- gRPC service and message definitions.
-   `BootstrapServer.java` --- worker registration, heartbeats,
    coordinator information, and monitoring.
-   `CoordinatorServer.java` --- starts the coordinator gRPC service.
-   `WorkerImpl.java` --- worker-side gRPC operations and
    election/completion handling.
-   `WorkerServer.java`, `WorkerServer2.java`, `WorkerServer3.java`,
    `WorkerServer4.java` --- worker server entry points.
-   `JobServiceImpl.java` --- job distribution and processing logic.
-   Client GUI source files --- submit jobs and display
    results/progress.

Names may vary slightly if the project has been reorganised.

## Notes for Group Members

-   Work from the same shared project version.
-   Do not share passwords, access tokens, or other private credentials.
-   After changing Java source or `.proto` files, run **Clean and
    Build** and retest affected functionality.
-   Record which tests were run and their results.
-   Update this README if ports, main classes, run arguments, or
    implemented features change.

## Current Status

The project builds successfully in the current development setup and has
demonstrated basic job execution, worker registration, progress
reporting, heartbeat-based failure detection, coordinator re-election,
and successful submission of a new job after coordinator failover.
Unfinished-task recovery and gradual real-time progress display still
require focused verification.
