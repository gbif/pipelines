package org.gbif.crawler.pipelines;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;

public class MessagePublisherStub implements MessagePublisher {

  private List<Message> messages = new ArrayList<>();

  private MessagePublisherStub() {
  }

  public static MessagePublisherStub create() {
    return new MessagePublisherStub();
  }

  @Override
  public void send(Message message) throws IOException {
    messages.add(message);
  }

  @Override
  public void send(Message message, boolean b) throws IOException {
    messages.add(message);
  }

  @Override
  public void send(Message message, String s) throws IOException {
    messages.add(message);
  }

  @Override
  public void send(Object o, String s, String s1) throws IOException {
    // NOP
  }

  @Override
  public void send(Object o, String s, String s1, boolean b) throws IOException {
    // NOP
  }

  @Override
  public void close() {
    messages = new ArrayList<>();
  }

  public List<Message> getMessages() {
    return messages;
  }
}
