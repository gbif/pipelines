package org.gbif.pipelines.maven;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class XmlToAvscGeneratorMojoTest {

  @Test
  public void mojoTest() throws Exception {

    // State
    String path = getClass().getResource("/generated/").getFile();

    XmlToAvscGeneratorMojo mojo = new XmlToAvscGeneratorMojo();
    mojo.setNamespace("org.gbif.pipelines.io.avro");
    mojo.setPathToWrite(path);

    // When
    mojo.execute();

    // Should
    Path result = Paths.get(path, "identification-table.avsc");
    Assert.assertTrue(Files.exists(result));

    Schema schema = new Schema.Parser().parse(result.toFile());
    Assert.assertEquals("IdentificationTable", schema.getName());
    Assert.assertEquals("org.gbif.pipelines.io.avro.dwc", schema.getNamespace());
    Assert.assertEquals(78, schema.getFields().size());
    Assert.assertNotNull(schema.getField("order_"));
  }
}
