package org.gbif.validator.it.mocks;

import java.io.IOException;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;

public class MessagePublisherMock implements MessagePublisher {

  @Override
  public void send(Message message) throws IOException {}

  @Override
  public void send(Message message, boolean persistent) throws IOException {}

  @Override
  public void send(Message message, String exchange) throws IOException {}

  @Override
  public void send(Object message, String exchange, String routingKey) throws IOException {}

  @Override
  public void send(Object message, String exchange, String routingKey, boolean persistent)
      throws IOException {}

  @Override
  public void replyToQueue(Object message, boolean persistent, String correlationId, String replyTo)
      throws IOException {}

  @Override
  public <T> T sendAndReceive(Message message, String s, boolean b, String s1)
      throws IOException, InterruptedException {
    return null;
  }

  @Override
  public <T> T sendAndReceive(Object o, String s, String s1, boolean b, String s2)
      throws IOException, InterruptedException {
    return null;
  }

  @Override
  public void close() {}
}
