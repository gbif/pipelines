package org.gbif.pipelines.maven;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class OccurrenceAvscGeneratorMojoTest {

  @Test
  public void mojoTest() throws Exception {

    // State
    String path = getClass().getResource("/").getPath();
    String occurrenceFile = "occurrence-hdfs-record.avsc";
    String eventFile = "event-hdfs-record.avsc";

    OccurrenceAvscGeneratorMojo mojo = new OccurrenceAvscGeneratorMojo();
    mojo.setPathToWrite(Paths.get(path).toString());
    mojo.setOccurrenceFile(occurrenceFile);
    mojo.setEventFile(eventFile);

    // When
    mojo.execute();

    // Should
    Path occurrenceResult = Paths.get(path, occurrenceFile);
    Assert.assertTrue(Files.exists(occurrenceResult));

    Schema occurrenceSchema = new Schema.Parser().parse(occurrenceResult.toFile());
    Assert.assertEquals("OccurrenceHdfsRecord", occurrenceSchema.getName());
    Assert.assertEquals("org.gbif.pipelines.io.avro", occurrenceSchema.getNamespace());

    Path eventResult = Paths.get(path, eventFile);
    Assert.assertTrue(Files.exists(eventResult));

    Schema eventSchema = new Schema.Parser().parse(eventResult.toFile());
    Assert.assertEquals("EventHdfsRecord", eventSchema.getName());
    Assert.assertEquals("org.gbif.pipelines.io.avro.event", eventSchema.getNamespace());
  }
}
