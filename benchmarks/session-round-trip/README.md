# Round-trip latency of the Spring Boot 3 FIX pair

The benchmark sends FIX TestRequests on alternating FIX 4.2 and 4.4 sessions and times the matching Heartbeat at the client. The application callbacks and both QuickFIX/J file logs run throughout. JLBH and the timing observer are benchmark dependencies. The packaged applications use their normal runtime dependencies.

From the repository root, build the original example modules and the benchmark with Maven and JDK 17. Run the driver with Python 3.9 or newer, using a new output directory for each process start.

```sh
mvn -pl simple-client-spring-3,simple-server-spring-3 -am clean package
cd benchmarks/session-round-trip
mvn package dependency:build-classpath -Dmdep.outputFile=target/benchmark.cp
python3 run.py --rate 20000 --logging WARN --dispatch shared --output /var/tmp/fix-shared-1
python3 run.py --rate 20000 --logging WARN --dispatch session --output /var/tmp/fix-session-1
```

The runner starts the packaged server and the example client with a test observer on its classpath. It assigns temporary ports, waits for logon and verifies the active initiator class. Both application JVMs use 512 MiB heaps. By default 12,000 paced warm-up requests precede five five-second windows. `--rate` is the aggregate rate across both sessions.

Measure INFO to include application-level message formatting. WARN keeps FIX file recording and lifecycle events. Shared dispatch uses `SocketInitiator` and per-session dispatch uses `ThreadedSocketInitiator`. The latter allows concurrent callbacks on different sessions.

The runner exits unsuccessfully on a missing, duplicate or wrong-session response, a broken schedule, or a missing request or Heartbeat in either peer's file log. The output includes individual deadlines and reply timestamps, exact per-session and combined percentiles, the median across windows, the pooled values, file-log hashes and the actual initiator class. Run configurations serially on an idle host.

## Measured results

The CSV gives the median of five window percentiles in microseconds, with both sessions combined. Each configuration started fresh on an AMD Ryzen 9 9955HX3D with Linux and OpenJDK 17, 512 MiB application heaps and a 12,000-request warm-up. Each rate is the total TestRequests/s across the two sessions. The version comparison used the shared client dispatcher. The base build used QuickFIX/J 2.3.1 and the updated build used 2.3.3. INFO and quiet select INFO and WARN application message logging, respectively. The dispatcher comparison used 2.3.3 and WARN. Each configuration ran five five-second windows. Both peers recorded FIX messages on ext4. [Measured percentiles](median-percentiles-2026-11-27.csv)

To reproduce the version comparison, build the two application modules with `mvn -pl simple-client-spring-3,simple-server-spring-3 -am -Dspring3.quickfixj.version=2.3.1 clean package`. Run the benchmark at the desired rate and log level with shared dispatch. Repeat the same build with `-Dspring3.quickfixj.version=2.3.3` and use a fresh output directory. The property selects both the engine and its message dictionaries.
