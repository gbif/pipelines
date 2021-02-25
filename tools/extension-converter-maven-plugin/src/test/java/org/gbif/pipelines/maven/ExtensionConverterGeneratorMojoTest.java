package org.gbif.pipelines.maven;

import org.junit.Test;

public class ExtensionConverterGeneratorMojoTest {

  @Test
  public void t() throws Exception {
    new ExtensionConverterGeneratorMojo().execute();
  }
}
