/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.validator.ws.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.api.MessagePublisher;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class MessagingConfiguration {

  @Bean
  @ConditionalOnProperty(value = "messaging.enabled", havingValue = "true")
  public MessagePublisher messagePublisher(
      ObjectMapper objectMapper, RabbitProperties rabbitProperties) throws IOException {
    log.info("DefaultMessagePublisher activated");
    return new DefaultMessagePublisher(
        new ConnectionParameters(
            rabbitProperties.getHost(),
            rabbitProperties.getPort(),
            rabbitProperties.getUsername(),
            rabbitProperties.getPassword(),
            rabbitProperties.getVirtualHost()),
        new DefaultMessageRegistry(),
        objectMapper);
  }
}
