package org.gbif.pipelines.coordinator;

import java.io.IOException;
import org.gbif.common.messaging.api.MessageCallback;

public interface CloseableMessageCallback<I> extends MessageCallback<I>, AutoCloseable {

  boolean isRunning();

  int getRunningCounter();

  void init() throws IOException;
}
