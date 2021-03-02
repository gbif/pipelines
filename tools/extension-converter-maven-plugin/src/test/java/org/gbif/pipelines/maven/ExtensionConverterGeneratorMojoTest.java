package org.gbif.pipelines.maven;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class ExtensionConverterGeneratorMojoTest {

  @Test
  public void mojoTest() throws Exception {

    // State
    String path = getClass().getResource("/").getFile();
    Path javaGenerated = Files.createDirectories(Paths.get(path, "java-generated"));

    ExtensionConverterGeneratorMojo mojo = new ExtensionConverterGeneratorMojo();
    mojo.setExtensions(
        Collections.singletonList(
            "MeasurementOrFactTable,https://rs.gbif.org/extension/dwc/measurements_or_facts.xml"));
    mojo.setAvroNamespace("org.gbif.pipelines.io.avro");
    mojo.setPackagePath("org.gbif.pipelines.core.converters");
    mojo.setPathToWrite(javaGenerated.toString());

    // When
    mojo.execute();

    // Should
    Path result =
        Paths.get(
            javaGenerated.toString(),
            "org/gbif/pipelines/core/converters/MeasurementOrFactTableConverter.java");
    Assert.assertTrue(Files.exists(result));
  }
}
