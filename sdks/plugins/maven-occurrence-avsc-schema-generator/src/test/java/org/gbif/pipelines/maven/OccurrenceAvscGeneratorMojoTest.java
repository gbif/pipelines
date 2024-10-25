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
    String name = "occurrence-hdfs-record.avsc";

    OccurrenceAvscGeneratorMojo mojo = new OccurrenceAvscGeneratorMojo();
    mojo.setPathToWrite(Paths.get(path, name).toString());

    // When
    mojo.execute();

    // Should
    Path result = Paths.get(path, "occurrence-hdfs-record.avsc");
    Assert.assertTrue(Files.exists(result));

    Schema schema = new Schema.Parser().parse(result.toFile());
    Assert.assertEquals("OccurrenceHdfsRecord", schema.getName());
    Assert.assertEquals("org.gbif.pipelines.io.avro", schema.getNamespace());
  }
}
