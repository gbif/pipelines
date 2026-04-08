package org.gbif.pipelines.healthcheck;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Health check utility that monitors a RabbitMQ queue and a Prometheus metric to determine service
 * health. This has been written to be used in Kubernetes liveness/readiness probes. The service is
 * considered unhealthy if there are messages in the RabbitMQ queue that have not been consumed for
 * a specified threshold duration.
 */
public final class HealthCheck {

  private static final String RABBITMQ_URL = requireEnv("RABBITMQ_URL");
  private static final String RABBITMQ_USER = requireEnv("RABBITMQ_USER");
  private static final String RABBITMQ_PASSWORD = requireEnv("RABBITMQ_PASSWORD");
  private static final String PROMETHEUS_URL =
      System.getenv().getOrDefault("PROMETHEUS_URL", "http://localhost:9404/metrics");
  private static final int STALE_THRESHOLD_SECONDS =
      Integer.parseInt(System.getenv().getOrDefault("STALE_THRESHOLD_SECONDS", "3600"));
  private static final int CHECK_INTERVAL_SECONDS =
      Integer.parseInt(System.getenv().getOrDefault("CHECK_INTERVAL_SECONDS", "30"));
  private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
  public static final String LAST_CONSUMED_MESSAGE_TIMESTAMP_MILLISECONDS =
      System.getenv()
          .getOrDefault(
              "PROMETHEUS_TIMESTAMP_TO_CHECK", "last_consumed_message_timestamp_milliseconds");

  public static final String READ_FROM_QUEUE_COUNTER =
      System.getenv().getOrDefault("PROMETHEUS_MESSAGE_COUNTER", "messages_read_from_queue_total");

  private static volatile boolean healthy = true;
  private static volatile String debug = "NOT_SET";

  private static final HttpClient httpClient =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

  public static void main(String[] args) throws Exception {
    System.out.println(
        "Starting health checker - stale threshold seconds: " + STALE_THRESHOLD_SECONDS);
    startHealthCheckLoop();
    startHttpServer();
  }

  private static void startHealthCheckLoop() {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduler.scheduleAtFixedRate(
        () -> {
          try {
            doHeathCheck();
          } catch (Exception e) {
            healthy = false;
          }
        },
        0,
        CHECK_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
  }

  /**
   * Performs a single health check by querying RabbitMQ and Prometheus.
   *
   * @throws Exception
   */
  private static void doHeathCheck() throws Exception {
    try {
      int messagesReady = fetchRabbitMqMessagesReady();
      String body = fetchPrometheusMetrics();

      Optional<Long> messagesReadFromQueue = parsePrometheusLong(body, READ_FROM_QUEUE_COUNTER);
      Optional<Long> lastConsumedTimestamp =
          parsePrometheusLong(body, LAST_CONSUMED_MESSAGE_TIMESTAMP_MILLISECONDS);

      // if the app has just started up and there is no metric yet, consider it healthy
      if (messagesReadFromQueue.isEmpty()) {
        healthy = true;
        debug =
            "No messages_read_from_queue_total metric - assume still starting up or no messages to queue";
        return;
      }

      if (lastConsumedTimestamp.isEmpty()) {
        debug = "Missing last_consumed_message_timestamp_milliseconds metric";
        healthy = false;
        return;
      }

      long timeSinceLastMessageDequeued =
          Instant.now().getEpochSecond() - (lastConsumedTimestamp.get() / 1000);

      healthy = !(messagesReady > 0 && timeSinceLastMessageDequeued > STALE_THRESHOLD_SECONDS);
      debug =
          String.format(
              "Queued messages_ready=%d, last consumed age=%d seconds%n",
              messagesReady, timeSinceLastMessageDequeued);

    } catch (Exception e) {
      StringWriter s = new java.io.StringWriter();
      PrintWriter w = new java.io.PrintWriter(s);
      e.printStackTrace(w);
      debug = System.currentTimeMillis() + " Exception during health check: " + s;
      throw e;
    }
  }

  private static int fetchRabbitMqMessagesReady() throws Exception {
    String auth =
        Base64.getEncoder()
            .encodeToString(
                (RABBITMQ_USER + ":" + RABBITMQ_PASSWORD).getBytes(StandardCharsets.UTF_8));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(RABBITMQ_URL))
            .timeout(Duration.ofSeconds(3))
            .header("Authorization", "Basic " + auth)
            .header("Accept", "application/json")
            .GET()
            .build();

    HttpResponse<InputStream> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      System.err.println("RabbitMQ status code" + response.statusCode());
      throw new IOException("RabbitMQ non-2xx response");
    }

    String body = new String(response.body().readAllBytes());
    return parseJsonInt(body, "messages_ready");
  }

  private static String fetchPrometheusMetrics() throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(PROMETHEUS_URL))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .build();

    HttpResponse<InputStream> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      System.err.println("Prometheus metrics URL status code" + response.statusCode());
      throw new IOException("Prometheus metrics non-2xx response");
    }

    return new String(response.body().readAllBytes());
  }

  private static int parseJsonInt(String json, String field) {
    String key = "\"" + field + "\"";
    int idx = json.indexOf(key);
    if (idx < 0) {
      throw new IllegalArgumentException("Missing field: " + field);
    }

    int colon = json.indexOf(':', idx);
    int i = colon + 1;
    while (Character.isWhitespace(json.charAt(i))) i++;

    int start = i;
    while (Character.isDigit(json.charAt(i))) i++;

    return Integer.parseInt(json.substring(start, i));
  }

  private static Optional<Long> parsePrometheusLong(String metrics, String metricName) {
    for (String line : metrics.split("\n")) {
      if (line.startsWith(metricName + " ")) {
        String[] parts = line.split("\\s+");
        double timeDouble = Double.parseDouble(parts[1]);
        return Optional.of((long) timeDouble);
      }
    }
    return Optional.empty();
  }

  private static void startHttpServer() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 0);
    server.createContext("/", new HealthHandler());
    server.createContext("/debug", new DebugHealthHandler());
    server.setExecutor(Executors.newSingleThreadExecutor());
    server.start();
  }

  private static final class HealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if (!"GET".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        return;
      }

      int status = healthy ? 200 : 500;
      byte[] body = healthy ? "OK\n".getBytes() : "UNHEALTHY\n".getBytes();

      exchange.sendResponseHeaders(status, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    }
  }

  private static final class DebugHealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if (!"GET".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        return;
      }
      byte[] body = debug.getBytes();
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    }
  }

  private static String requireEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required env var: " + name);
    }
    return value;
  }
}
