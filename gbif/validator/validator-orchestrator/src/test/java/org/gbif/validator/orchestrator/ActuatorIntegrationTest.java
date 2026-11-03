package org.gbif.validator.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.validator.orchestrator.support.RecordingValidationClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/** Starts the production application and its real Actuator without external services. */
@SpringBootTest(
    classes = Main.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "orchestrator.unsafe-enable-start-consumer=false",
      "orchestrator.unsafe-enable-completion-consumer=false",
      "orchestrator.validator-ws.url=http://127.0.0.1:1",
      "orchestrator.validator-ws.username=test",
      "orchestrator.validator-ws.password=test",
      "management.endpoint.health.probes.enabled=true",
      "management.endpoints.web.exposure.include=health,metrics"
    })
@Import(ActuatorIntegrationTest.OfflineInfrastructure.class)
class ActuatorIntegrationTest {

  @LocalServerPort private int applicationPort;

  @Autowired private MeterRegistry registry;
  @Autowired private ObjectMapper mapper;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

  @Test
  void actuatorHealthProbesAreUp() throws Exception {
    assertEquals("UP", getJson("/actuator/health/liveness").path("status").asText());
    assertEquals("UP", getJson("/actuator/health/readiness").path("status").asText());
  }

  @Test
  void registeredMetricsAreVisibleThroughActuator() throws Exception {
    String name = "validator.orchestrator.integration.probe";
    Counter counter = registry.counter(name, "test", "actuator");
    double before = counter.count();

    counter.increment(2.0);

    assertEquals(before + 2.0, counter.count(), 0.0001);
    assertNotNull(registry.find(name).tags("test", "actuator").counter());

    JsonNode result = getJson("/actuator/metrics/" + name);
    assertEquals(name, result.path("name").asText());
    assertTrue(result.path("measurements").isArray());
    assertFalse(result.path("measurements").isEmpty());

    boolean found = false;
    for (JsonNode measurement : result.path("measurements")) {
      if ("COUNT".equals(measurement.path("statistic").asText())) {
        assertEquals(counter.count(), measurement.path("value").asDouble(), 0.0001);
        found = true;
      }
    }
    assertTrue(found, "Actuator must expose the counter count");
  }

  private JsonNode getJson(String path) throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + applicationPort + path))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();
    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, response.statusCode(), path + " returned " + response.body());
    return mapper.readTree(response.body());
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class OfflineInfrastructure {

    /** Remove definitions before singleton creation; @Primary alone would still connect. */
    @Bean
    static BeanDefinitionRegistryPostProcessor suppressExternalInfrastructure() {
      return new BeanDefinitionRegistryPostProcessor() {
        @Override
        public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry)
            throws BeansException {
          for (String beanName :
              new String[] {"messagePublisher", "messageListener", "validationWsClient"}) {
            if (!registry.containsBeanDefinition(beanName)) {
              throw new IllegalStateException("Expected production bean: " + beanName);
            }
            registry.removeBeanDefinition(beanName);
          }
        }

        @Override
        public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory)
            throws BeansException {
          // No factory changes are necessary.
        }
      };
    }

    @Bean
    MessagePublisher offlineMessagePublisher() {
      return (MessagePublisher)
          Proxy.newProxyInstance(
              MessagePublisher.class.getClassLoader(),
              new Class<?>[] {MessagePublisher.class},
              (proxy, method, args) -> {
                throw new AssertionError("Unexpected RabbitMQ publication: " + method.getName());
              });
    }

    @Bean
    ValidationWsClient offlineValidationWsClient() {
      return new RecordingValidationClient();
    }
  }
}
