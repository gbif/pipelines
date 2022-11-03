package org.gbif.pipelines.diagnostics.tools;

import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

public class RecordIDsByCrawlAttemptToolTest {

  @Test
  public void emptyTest() {

    // State
    String file = this.getClass().getResource("/").getFile();
    String tmp = this.getClass().getResource("/").getFile();

    // When
    RecordIDsByCrawlAttemptTool.builder()
        .catalogNumber("PERTH 3994120")
        .occurrenceID("01c2a4e5-eee9-43c5-a2e8-da562711e9b8")
        .directory(Paths.get(file).toFile())
        .tmp(Paths.get(tmp).toFile())
        .build()
        .run();

    Assert.assertTrue(true);
  }
}
