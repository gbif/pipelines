package org.gbif.pipelines.maven;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

public class ExtensionConverterGeneratorMojoTest {

  @Test
  public void mojoTest() throws Exception {

    // State
    String path = getClass().getResource("/").getFile();
    Path javaGenerated = Files.createDirectories(Paths.get(path, "java-generated"));

    ExtensionConverterGeneratorMojo mojo = new ExtensionConverterGeneratorMojo();
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

  @Test
  public void normalizeNameTest() {
    // State
    String name = "lib_reads_seqd";

    // When
    String result = new ExtensionConverterGeneratorMojo().normalizeName(name);

    // Should
    Assert.assertEquals("Libreadsseqd", result);
  }
}
