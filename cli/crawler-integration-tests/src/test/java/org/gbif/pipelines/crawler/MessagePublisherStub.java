package org.gbif.pipelines.crawler;

import java.util.ArrayList;
import java.util.List;

import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;

import lombok.Getter;
import lombok.NoArgsConstructor;

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
  public void close() {
    messages = new ArrayList<>();
  }
}
