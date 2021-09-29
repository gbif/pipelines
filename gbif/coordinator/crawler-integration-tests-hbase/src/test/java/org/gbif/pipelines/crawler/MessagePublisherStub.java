package org.gbif.pipelines.crawler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;

@Getter
@NoArgsConstructor(staticName = "create")
public class MessagePublisherStub implements MessagePublisher {

  private List<Message> messages = new ArrayList<>();

  @Override
  public void send(Message message) {
    messages.add(message);
  }

  @Override
  public void send(Message message, boolean b) {
    messages.add(message);
  }

  @Override
  public void send(Message message, String s) {
    messages.add(message);
  }

  @Override
  public void send(Object o, String s, String s1) {
    // NOP
  }

  @Override
  public void send(Object o, String s, String s1, boolean b) {
    // NOP
  }

  @Override
  public void replyToQueue(Object message, boolean persistent, String correlationId, String replyTo)
      throws IOException {
    // NOP
  }

  @Override
  public <T> void sendAndReceive(
      Message message,
      String routingKey,
      boolean persistent,
      String correlationId,
      String replyTo,
      Consumer<T> consumer)
      throws IOException {
    // NOP
  }

  @Override
  public <T> void sendAndReceive(
      Object message,
      String exchange,
      String routingKey,
      boolean persistent,
      String correlationId,
      String replyTo,
      Consumer<T> consumer)
      throws IOException {
    // NOP
  }

  @Override
  public void close() {
    messages = new ArrayList<>();
  }
}
