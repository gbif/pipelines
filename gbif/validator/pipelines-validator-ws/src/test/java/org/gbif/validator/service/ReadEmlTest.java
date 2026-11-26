package org.gbif.validator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gbif.api.model.registry.Dataset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReadEmlTest {

  private static final String EML =
      "<?xml version=\"1.0\"?>%s<eml:eml xmlns:eml=\"eml://ecoinformatics.org/eml-2.1.1\""
          + " packageId=\"p\" system=\"s\"><dataset><title>%s</title></dataset></eml:eml>";

  @Test
  void readsEml(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("eml.xml"), String.format(EML, "", "My dataset"));

    Dataset dataset = ValidationServiceImpl.readEml(dir);

    assertEquals("My dataset", dataset.getTitle());
  }

  @Test
  void externalEntitiesAreNotResolved(@TempDir Path dir) throws Exception {
    Path secret = Files.writeString(dir.resolve("secret.txt"), "very-secret-value");
    Path archive = Files.createDirectories(dir.resolve("archive"));
    String doctype = "<!DOCTYPE eml:eml [<!ENTITY x SYSTEM \"" + secret.toUri() + "\">]>";
    Files.writeString(archive.resolve("eml.xml"), String.format(EML, doctype, "&x;"));

    assertNull(ValidationServiceImpl.readEml(archive));
  }
}
