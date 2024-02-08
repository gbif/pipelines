package org.gbif.pipelines.maven;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class AvroPostprocessMojoTest {

  @Test
  public void mojoTest() throws Exception {

    // State
    String path = getClass().getResource("/generated/").getFile();

    // When
    AvroPostprocessMojo mojo = new AvroPostprocessMojo();
    mojo.setDefaultPackage("org.gbif.pipelines.io.avro");
    mojo.setDirectory(path);
    mojo.execute();

    // Should
    Path issuePath = Paths.get(path + "org/gbif/pipelines/io/avro/Issues.java");
    Assert.assertTrue(Files.exists(issuePath));

    Path recordPath = Paths.get(path + "org/gbif/pipelines/io/avro/Record.java");
    Assert.assertTrue(Files.exists(recordPath));

    Path testPath = Paths.get(path + "org/gbif/pipelines/io/avro/TestRecord.java");
    Assert.assertTrue(Files.exists(testPath));

    List<String> lines = Files.readAllLines(testPath);
    Assert.assertTrue(
        lines.contains("import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;"));
    Assert.assertTrue(lines.contains("import org.apache.beam.sdk.coders.DefaultCoder;"));
    Assert.assertTrue(lines.contains("@DefaultCoder(AvroCoder.class)"));
    Assert.assertTrue(
        lines.contains(
            "public class TestRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord, org.gbif.pipelines.io.avro.Issues, org.gbif.pipelines.io.avro.Record {"));
  }
}
