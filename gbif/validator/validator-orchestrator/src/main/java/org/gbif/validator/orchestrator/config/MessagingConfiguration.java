package org.gbif.validator.orchestrator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Messaging connection is configurable independently of step destinations. */
@Configuration
public class MessagingConfiguration {
  @Bean
  public ConnectionParameters connectionParameters(RabbitProperties rabbit) {
    return new ConnectionParameters(
        rabbit.getHost(),
        rabbit.getPort(),
        rabbit.getUsername(),
        rabbit.getPassword(),
        rabbit.getVirtualHost());
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
