import io.allune.quickfixj.spring.boot.starter.examples.client.SimpleClientSpring3;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import net.openhft.chronicle.jlbh.*;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import quickfix.*;

public class SessionRoundTrip implements JLBHTask {
  static SessionRoundTrip task;
  final SessionID[] lanes = {
    new SessionID("FIX.4.2", "BANZAI", "EXEC"), new SessionID("FIX.4.4", "BANZAI", "EXEC")
  };
  final int warm, rate, count, runs;
  final long[] due, tick, ack;
  final AtomicInteger offered = new AtomicInteger();
  int received, invalid;
  volatile JLBH harness;

  SessionRoundTrip(int warm, int rate, int count, int runs) {
    this.warm = warm;
    this.rate = rate;
    this.count = count;
    this.runs = runs;
    due = new long[Math.addExact(warm, Math.multiplyExact(count, runs))];
    tick = new long[due.length];
    ack = new long[due.length];
  }

  public void init(JLBH jlbh) {
    harness = jlbh;
  }

  public void run(long start) {
    int id = offered.getAndIncrement();
    int slot = id < warm ? id : (id - warm) % count;
    tick[id] = start;
    due[id] = slot == 0 ? start : due[id - slot] + slot * 1_000_000_000L / rate;
    if (id < warm) {
      long delay;
      while ((delay = due[id] - System.nanoTime()) > 0) LockSupport.parkNanos(delay);
    }
    Message request = new Message();
    request.getHeader().setString(35, "1");
    request.setString(112, "PT" + id);
    try {
      if (!Session.sendToTarget(request, lanes[id % 2]))
        throw new IllegalStateException("send refused " + id);
    } catch (SessionNotFound e) {
      throw new IllegalStateException(e);
    }
  }

  long next(long interval) {
    int id = offered.get();
    if (id <= warm || (id - warm) % count == 0) return interval;
    int slot = (id - warm) % count;
    return Math.max(0, due[id - slot] + slot * 1_000_000_000L / rate - tick[id - 1]);
  }

  synchronized void onReply(Message message, SessionID session) throws FieldNotFound {
    if (!"0".equals(message.getHeader().getString(35)) || !message.isSetField(112)) return;
    String value = message.getString(112);
    if (!value.startsWith("PT")) return;
    int id = Integer.parseInt(value.substring(2));
    if (id < 0
        || id >= due.length
        || !lanes[id % 2].equals(session)
        || ack[id] != 0
        || due[id] == 0) {
      invalid++;
      return;
    }
    ack[id] = System.nanoTime();
    received++;
    harness.sample(ack[id] - due[id]);
  }

  // Forward every callback to the packaged example. Measure after its fromAdmin returns.
  static class Observed implements Application {
    final Application app;

    Observed(Application app) {
      this.app = app;
    }

    public void onCreate(SessionID id) {
      app.onCreate(id);
    }

    public void onLogon(SessionID id) {
      app.onLogon(id);
    }

    public void onLogout(SessionID id) {
      app.onLogout(id);
    }

    public void toAdmin(Message m, SessionID id) {
      app.toAdmin(m, id);
    }

    public void toApp(Message m, SessionID id) throws DoNotSend {
      app.toApp(m, id);
    }

    public void fromAdmin(Message m, SessionID id)
        throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
      app.fromAdmin(m, id);
      task.onReply(m, id);
    }

    public void fromApp(Message m, SessionID id)
        throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
      app.fromApp(m, id);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class Observe {
    @Bean
    static BeanPostProcessor observer() {
      return new BeanPostProcessor() {
        public Object postProcessAfterInitialization(Object bean, String name) {
          return name.equals("clientApplication") ? new Observed((Application) bean) : bean;
        }
      };
    }
  }

  public static void main(String[] args) throws Exception {
    task =
        new SessionRoundTrip(
            Integer.parseInt(args[0]),
            Integer.parseInt(args[1]),
            Integer.parseInt(args[2]),
            Integer.parseInt(args[3]));
    try (var context =
        new SpringApplicationBuilder(SimpleClientSpring3.class, Observe.class)
            .run("--server.port=0")) {
      String connector = context.getBean(Initiator.class).getClass().getName();
      String expected =
          Boolean.parseBoolean(System.getenv("QUICKFIXJ_CLIENT_CONCURRENT_ENABLED"))
              ? ThreadedSocketInitiator.class.getName()
              : SocketInitiator.class.getName();
      if (!expected.equals(connector))
        throw new IllegalStateException("Expected " + expected + ", got " + connector);
      Files.writeString(Path.of("initiator.txt"), connector + "\n");
      for (SessionID lane : task.lanes) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (Session.lookupSession(lane) == null || !Session.lookupSession(lane).isLoggedOn()) {
          if (System.nanoTime() > deadline)
            throw new IllegalStateException("logon timed out " + lane);
          Thread.sleep(20);
        }
      }
      var options =
          new JLBHOptions()
              .recordOSJitter(false)
              .warmUpIterations(task.warm)
              .iterations(task.count)
              .throughput(task.rate)
              .runs(task.runs)
              .latencyDistributor(task::next)
              .timeout(120_000L)
              .accountForCoordinatedOmission(true)
              .jlbhTask(task);
      JLBH jlbh = new JLBH(options);
      var watchdog = Executors.newSingleThreadScheduledExecutor();
      watchdog.schedule(jlbh::abort, 120L + task.due.length / task.rate, TimeUnit.SECONDS);
      try {
        jlbh.start();
      } finally {
        watchdog.shutdownNow();
      }
      try (BufferedWriter writer = Files.newBufferedWriter(Path.of("samples.tsv"))) {
        writer.write("id\tdue_ns\tack_ns\n");
        for (int i = 0; i < task.due.length; i++)
          writer.write(i + "\t" + task.due[i] + "\t" + task.ack[i] + "\n");
      }
      if (task.invalid != 0
          || task.received != task.due.length
          || task.offered.get() != task.due.length)
        throw new IllegalStateException(
            "offered=" + task.offered + " received=" + task.received + " bad=" + task.invalid);
    }
  }
}
