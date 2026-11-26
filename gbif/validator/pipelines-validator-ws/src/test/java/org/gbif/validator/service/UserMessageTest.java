package org.gbif.validator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class UserMessageTest {

  @Test
  void internalErrorsAreNotShown() {
    assertEquals(
        "The file could not be processed",
        ValidationServiceImpl.userMessage(new IOException("/internal/path/x failed")));
    assertEquals(
        "The file could not be processed",
        ValidationServiceImpl.userMessage(
            new CompletionException(new IllegalStateException("db host 10.0.0.1"))));
  }

  @Test
  void fileErrorsAreShown() {
    assertEquals(
        "Unsupported format",
        ValidationServiceImpl.userMessage(
            new CompletionException(new IllegalArgumentException("Unsupported format"))));
  }
}
