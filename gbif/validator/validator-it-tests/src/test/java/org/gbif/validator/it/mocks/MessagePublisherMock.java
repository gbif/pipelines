package org.gbif.validator.it.mocks;

import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;

public class MessagePublisherMock implements MessagePublisher {

  @Override
  public void send(Message message) {}

  @Override
  public void send(Message message, boolean persistent) {}

  @Override
  public void send(Message message, String exchange) {}

  @Override
  public void send(Object message, String exchange, String routingKey) {}

  @Override
  public void send(Object message, String exchange, String routingKey, boolean persistent) {}

  @Override
  public void replyToQueue(
      Object message, boolean persistent, String correlationId, String replyTo) {}

  @Override
  public <T> T sendAndReceive(Message message, String s, boolean b, String s1) {
    return null;
  }

  @Override
  public <T> T sendAndReceive(Object o, String s, String s1, boolean b, String s2) {
    return null;
  }

  @Override
  public void close() {}
}
