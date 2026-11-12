package org.gbif.validator.orchestrator.config;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Maps pipeline step types to RabbitMQ request and completion destinations.
 *
 * <p>The connection (including vhost) is configured separately through the RabbitMQ connection
 * properties. Exchange, routing keys and completion queues can therefore be changed independently
 * for each deployed orchestrator.
 */
@ConfigurationProperties(prefix = "orchestrator")
public class OrchestratorProperties {

  private Map<StepType, StepMessaging> steps = new EnumMap<>(StepType.class);

  public Map<StepType, StepMessaging> getSteps() {
    return steps;
  }

  public void setSteps(Map<StepType, StepMessaging> steps) {
    this.steps = Objects.requireNonNull(steps, "steps must not be null");
  }

  public StepMessaging requireStep(StepType stepType) {
    StepMessaging messaging = steps.get(stepType);
    if (messaging == null) {
      throw new IllegalArgumentException("No messaging configured for step " + stepType);
    }
    return messaging;
  }

  public static class StepMessaging {
    private Destination request = new Destination();
    private Completion completion = new Completion();
    private Destination outgoing = new Destination();

    public Destination getRequest() {
      return request;
    }

    public void setRequest(Destination request) {
      this.request = request;
    }

    public Destination getOutgoing() {
      return outgoing;
    }

    public void setOutgoing(Destination outgoing) {
      this.outgoing = outgoing;
    }

    public Completion getCompletion() {
      return completion;
    }

    public void setCompletion(Completion completion) {
      this.completion = completion;
    }
  }

  public static class Destination {
    private String exchange;
    private String routingKey;

    public String getExchange() {
      return exchange;
    }

    public void setExchange(String exchange) {
      this.exchange = exchange;
    }

    public String getRoutingKey() {
      return routingKey;
    }

    public void setRoutingKey(String routingKey) {
      this.routingKey = routingKey;
    }
  }

  public static class Completion extends Destination {
    private String queue;
    private int threads = 1;

    public String getQueue() {
      return queue;
    }

    public void setQueue(String queue) {
      this.queue = queue;
    }

    public int getThreads() {
      return threads;
    }

    public void setThreads(int threads) {
      this.threads = threads;
    }
  }
}
