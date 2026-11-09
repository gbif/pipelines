package org.gbif.validator.orchestrator.config;

import org.gbif.common.messaging.MessageListener;
import org.gbif.validator.orchestrator.service.StepCompletionCallback;
import org.gbif.validator.orchestrator.step.CompletionProcessor;
import org.gbif.validator.orchestrator.step.StepRegistration;
import org.gbif.validator.orchestrator.step.StepRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Registers one typed completion consumer for every registered step. */
@Configuration
public class StepCompletionListenerConfiguration {

  private static final Logger LOG =
      LoggerFactory.getLogger(StepCompletionListenerConfiguration.class);

  @Bean
  public ApplicationRunner stepCompletionConsumers(
      MessageListener listener,
      OrchestratorProperties properties,
      StepRegistry registry,
      CompletionProcessor processor) {

    return args -> {
      // Check all destinations before creating any consumers.
      for (StepRegistration<?> registration : registry.registrations()) {
        validateRegistration(properties, registration);
      }

      int registered = 0;
      for (StepRegistration<?> registration : registry.registrations()) {
        register(listener, properties, registration, processor);
        registered++;
      }

      if (registered == 0) {
        throw new IllegalStateException("No step completion consumers registered");
      }

      LOG.info("Registered {} step completion consumers", registered);
    };
  }

  static void validateRegistration(
      OrchestratorProperties properties, StepRegistration<?> registration) {
    OrchestratorProperties.StepMessaging messaging =
        properties.requireStep(registration.stepType());
    requireDestination(messaging.getRequest(), "request", registration);
    requireCompletion(properties, registration);

    OrchestratorProperties.Destination outgoing = messaging.getOutgoing();
    if (registration.hasOutgoingAdapter()) {
      requireDestination(outgoing, "outgoing", registration);
    } else if (outgoing != null
        && (!isBlank(outgoing.getExchange()) || !isBlank(outgoing.getRoutingKey()))) {
      throw new IllegalStateException(
          "Outgoing destination without adapter for " + registration.stepType());
    }

    LOG.info(
        "Configured step={} ownership=orchestrator request={}/{} completion={}/{} queue={} threads={} outgoing={}/{}",
        registration.stepType(),
        messaging.getRequest().getExchange(),
        messaging.getRequest().getRoutingKey(),
        messaging.getCompletion().getExchange(),
        messaging.getCompletion().getRoutingKey(),
        messaging.getCompletion().getQueue(),
        messaging.getCompletion().getThreads(),
        outgoing == null ? null : outgoing.getExchange(),
        outgoing == null ? null : outgoing.getRoutingKey());
  }

  private static void requireDestination(
      OrchestratorProperties.Destination destination,
      String purpose,
      StepRegistration<?> registration) {
    if (destination == null
        || isBlank(destination.getExchange())
        || isBlank(destination.getRoutingKey())) {
      throw new IllegalStateException(
          "Invalid " + purpose + " destination for " + registration.stepType());
    }
  }

  private static OrchestratorProperties.Completion requireCompletion(
      OrchestratorProperties properties, StepRegistration<?> registration) {
    OrchestratorProperties.Completion completion =
        properties.requireStep(registration.stepType()).getCompletion();

    if (completion == null
        || isBlank(completion.getQueue())
        || isBlank(completion.getExchange())
        || isBlank(completion.getRoutingKey())
        || completion.getThreads() < 1) {
      throw new IllegalStateException(
          "Invalid completion destination for " + registration.stepType());
    }

    return completion;
  }

  private static <T> void register(
      MessageListener listener,
      OrchestratorProperties properties,
      StepRegistration<T> registration,
      CompletionProcessor processor)
      throws Exception {
    OrchestratorProperties.Completion completion = requireCompletion(properties, registration);

    StepCompletionCallback<T> callback =
        new StepCompletionCallback<>(
            registration.stepType(), registration.completionType(), processor::complete);

    listener.listen(
        completion.getQueue(),
        completion.getRoutingKey(),
        completion.getExchange(),
        completion.getThreads(),
        callback);
    LOG.info(
        "Listening for step completion step={} queue={} exchange={} routingKey={} threads={}",
        registration.stepType(),
        completion.getQueue(),
        completion.getExchange(),
        completion.getRoutingKey(),
        completion.getThreads());
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
