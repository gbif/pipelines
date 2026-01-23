package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.ConfigUtil.loadConfig;
import static org.gbif.pipelines.interpretation.spark.Directories.*;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.core.config.model.MessagingConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.jetbrains.annotations.NotNull;

/** The runnable */
@Slf4j
public class Standalone {

  //  static {
  //    DefaultExports.initialize();
  //  }

  private volatile boolean running = true;

  @Parameters(separators = "=")
  private static class Args {

    @Parameter(names = "--mode", description = "e.g. INTERPRETATION", required = true)
    private String mode;

    @Parameter(names = "--config", description = "Path to YAML configuration file")
    private String config = "/tmp/pipelines-spark.yaml";

    @Parameter(
        names = "--listenerThreads",
        description = "Number of queue listener threads (no of parallel messages processed)")
    private int threads = 1;

    @Parameter(names = "--master", description = "Master - relevant for embedded spark only")
    private String master = "local[*]";

    @Parameter(names = "--queueName", description = "queueName", required = true)
    private String queueName;

    @Parameter(names = "--routingKey", description = "routingKey", required = true)
    private String routingKey;

    @Parameter(names = "--exchange", description = "exchange")
    private String exchange = "occurrence";

    @Parameter(
        names = "--listenerThreadSleepMillis",
        description = "Number of millis to sleep for the listener thread")
    private long listenerThreadSleepMillis = 2000;

    @Parameter(names = "--prometheusPort", description = "metrics port. Set to 0 to disable")
    private int prometheusPort = 9404;
  }

  public static void main(String[] argsv) throws Exception {

    Standalone.Args args = new Standalone.Args();
    JCommander jCommander = new JCommander(args);
    jCommander.parse(argsv);
    Mode mode = Mode.valueOf(args.mode);
    PipelinesConfig config = loadConfig(args.config);

    JvmMetrics.builder().register();

    // start Prometheus HTTP server
    if (args.prometheusPort > 0) {
      log.info("Starting Prometheus HTTP server on port {}", args.prometheusPort);
      try (HTTPServer httpServer = HTTPServer.builder().port(args.prometheusPort).buildAndStart()) {
        new Standalone()
            .start(
                mode,
                config,
                args.queueName,
                args.routingKey,
                args.exchange,
                args.master,
                args.threads,
                args.listenerThreadSleepMillis);
      }
    } else {
      log.info("Prometheus HTTP server disabled");
      new Standalone()
          .start(
              mode,
              config,
              args.queueName,
              args.routingKey,
              args.exchange,
              args.master,
              args.threads,
              args.listenerThreadSleepMillis);
    }
  }

  public void start(
      Mode mode,
      PipelinesConfig config,
      String queueName,
      String routingKey,
      String exchange,
      String master,
      int threads,
      long threadSleepMillis) {

    Function<MessagePublisher, PipelinesCallback> callbackFn = null;

    switch (mode) {
      case IDENTIFIER:
        callbackFn = (messagePublisher -> new IdentifierCallback(config, messagePublisher, master));
        break;
      case IDENTIFIER_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new IdentifierDistributedCallback(config, messagePublisher));
        break;
      case INTERPRETATION:
        callbackFn =
            (messagePublisher -> new InterpretationCallback(config, messagePublisher, master));
        break;
      case INTERPRETATION_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new InterpretationDistributedCallback(config, messagePublisher));
        break;
      case EVENTS_INTERPRETATION:
        callbackFn =
            (messagePublisher ->
                new EventsInterpretationCallback(config, messagePublisher, master));
        break;
      case EVENTS_INTERPRETATION_DISTRIBUTED:
        callbackFn =
            (messagePublisher ->
                new EventsInterpretationDistributedCallback(config, messagePublisher));
        break;
      case TABLEBUILD:
        callbackFn =
            (messagePublisher ->
                new TableBuildCallback(
                    config, messagePublisher, master, "occurrence", OCCURRENCE_HDFS));
        break;
      case TABLEBUILD_DISTRIBUTED:
        callbackFn =
            (messagePublisher ->
                new TableBuildDistributedCallback(
                    config, messagePublisher, "occurrence", OCCURRENCE_HDFS));
        break;
      case EVENTS_TABLEBUILD:
        callbackFn =
            (messagePublisher ->
                new EventsTableBuildCallback(
                    config, messagePublisher, master, "event", EVENT_HDFS));
        break;
      case EVENTS_TABLEBUILD_DISTRIBUTED:
        callbackFn =
            (messagePublisher ->
                new EventsTableBuildDistributedCallback(
                    config, messagePublisher, "event", EVENT_HDFS));
        break;
      case INDEXING:
        callbackFn = (messagePublisher -> new IndexingCallback(config, messagePublisher, master));
        break;
      case INDEXING_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new IndexingDistributedCallback(config, messagePublisher));
        break;
      case EVENTS_INDEXING:
        callbackFn =
            (messagePublisher -> new EventsIndexingCallback(config, messagePublisher, master));
        break;
      case EVENTS_INDEXING_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new EventsIndexingDistributedCallback(config, messagePublisher));
        break;
      case FRAGMENTER:
        callbackFn = (messagePublisher -> new FragmenterCallback(config, messagePublisher, master));
        break;
      case FRAGMENTER_DISTRIBUTED:
        callbackFn =
            (messagePublisher -> new FragmenterDistributedCallback(config, messagePublisher));
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown mode: "
                + mode
                + ". Recognized modes are: "
                + Stream.of(Mode.values()).map(Enum::name).collect(Collectors.joining(",")));
    }

    log.info(
        "Running {}, listening to queue: {} on virtual host {}",
        mode,
        queueName,
        config.getStandalone().getMessaging().getVirtualHost());
    setupShutdown();

    try (MessageListener listener = createListener(config);
        DefaultMessagePublisher publisher = createPublisher(config);
        PipelinesCallback callback = callbackFn.apply(publisher)) {

      // initialise spark session & filesystem
      callback.init();

      // start the listener
      listener.listen(queueName, routingKey, exchange, threads, callback);

      // 5. Keep running until shutdown
      while (running) {
        try {
          Thread.sleep(threadSleepMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

    } catch (IOException e) {
      log.error("Error starting standalone", e);
    }

    log.info("Exiting Standalone.");
  }

  @NotNull
  private static MessageListener createListener(PipelinesConfig pipelinesConfig)
      throws IOException {
    MessagingConfig messagingConfig = pipelinesConfig.getStandalone().getMessaging();
    return new MessageListener(
        new ConnectionParameters(
            messagingConfig.getHost(),
            messagingConfig.getPort(),
            messagingConfig.getUsername(),
            messagingConfig.getPassword(),
            messagingConfig.getVirtualHost()),
        messagingConfig.getPrefetchCount());
  }

  @NotNull
  private static DefaultMessagePublisher createPublisher(PipelinesConfig pipelinesConfig)
      throws IOException {
    MessagingConfig messagingConfig = pipelinesConfig.getStandalone().getMessaging();
    return new DefaultMessagePublisher(
        new ConnectionParameters(
            messagingConfig.getHost(),
            messagingConfig.getPort(),
            messagingConfig.getUsername(),
            messagingConfig.getPassword(),
            messagingConfig.getVirtualHost()));
  }

  private void setupShutdown() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Shutdown signal received. Cleaning up...");
                  running = false;
                  log.info("Graceful shutdown complete.");
                }));
  }

  public enum Mode {
    IDENTIFIER,
    IDENTIFIER_DISTRIBUTED,
    INTERPRETATION,
    INTERPRETATION_DISTRIBUTED,
    EVENTS_INTERPRETATION,
    EVENTS_INTERPRETATION_DISTRIBUTED,
    TABLEBUILD,
    TABLEBUILD_DISTRIBUTED,
    EVENTS_TABLEBUILD,
    EVENTS_TABLEBUILD_DISTRIBUTED,
    INDEXING,
    INDEXING_DISTRIBUTED,
    EVENTS_INDEXING,
    EVENTS_INDEXING_DISTRIBUTED,
    FRAGMENTER,
    FRAGMENTER_DISTRIBUTED
  }
}
