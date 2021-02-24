package org.gbif.pipelines.maven;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class XmlToAvscGeneratorMojoTest {

  @Test
  public void xmlToAvscGeneratorMojoTest() throws Exception {

    // State
    String path = getClass().getResource("/generated/").getFile();

    XmlToAvscGeneratorMojo mojo = new XmlToAvscGeneratorMojo();
    mojo.setExtensions(
        Collections.singletonList(
            "MeasurementOrFactTable,https://rs.gbif.org/extension/dwc/measurements_or_facts.xml"));
    mojo.setNamespace("org.gbif.pipelines.io.avro");
    mojo.setPathToWrite(path);

    // When
    mojo.execute();

    // Should
    Path result = Paths.get(path, "measurement-or-fact-table.avsc");
    Assert.assertTrue(Files.exists(result));

    Schema schema = new Schema.Parser().parse(result.toFile());
    Assert.assertEquals("MeasurementOrFactTable", schema.getName());
    Assert.assertEquals("org.gbif.pipelines.io.avro", schema.getNamespace());
    Assert.assertEquals(19, schema.getFields().size());
  }
}
