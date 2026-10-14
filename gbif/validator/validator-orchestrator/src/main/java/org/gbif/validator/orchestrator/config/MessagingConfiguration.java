package org.gbif.validator.orchestrator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Messaging connection is configurable independently of step destinations. */
@Configuration
public class MessagingConfiguration {
  @Bean
  public ConnectionParameters connectionParameters(
      @Value("${spring.rabbitmq.host:localhost}") String host,
      @Value("${spring.rabbitmq.port:5672}") int port,
      @Value("${spring.rabbitmq.username:guest}") String username,
      @Value("${spring.rabbitmq.password:guest}") String password,
      @Value("${spring.rabbitmq.virtual-host:/}") String virtualHost) {

    return new ConnectionParameters(host, port, username, password, virtualHost);
  }

  @Bean
  public MessagePublisher messagePublisher(ConnectionParameters connection, ObjectMapper mapper)
      throws IOException {
    return new DefaultMessagePublisher(connection, new DefaultMessageRegistry(), mapper);
  }

  @Bean(destroyMethod = "close")
  public MessageListener messageListener(ConnectionParameters connection, ObjectMapper mapper)
      throws IOException {
    return new MessageListener(connection, new DefaultMessageRegistry(), mapper);
  }
}
