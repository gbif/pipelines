package org.gbif.dwca.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetadataPathTest {

  private static Path archive(Path dir, String metadata) throws Exception {
    Path archive = Files.createDirectories(dir.resolve("archive"));
    Files.writeString(
        archive.resolve("meta.xml"),
        "<archive xmlns=\"http://rs.tdwg.org/dwc/text/\" metadata=\"" + metadata + "\"/>");
    return archive;
  }

  @Test
  void metadataFromMetaXml(@TempDir Path dir) throws Exception {
    Path archive = archive(dir, "dataset.xml");
    Files.writeString(archive.resolve("dataset.xml"), "<eml/>");

    assertEquals(
        Optional.of(archive.resolve("dataset.xml").toAbsolutePath()),
        MetadataPath.parsePath(archive));
  }

  @Test
  void metadataOutsideTheArchiveIsIgnored(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("secret.xml"), "<secret/>");

    assertTrue(MetadataPath.parsePath(archive(dir, "../secret.xml")).isEmpty());
    assertTrue(
        MetadataPath.parsePath(archive(dir, dir.resolve("secret.xml").toString())).isEmpty());
    assertTrue(MetadataPath.parsePath(archive(dir, "/dev/zero")).isEmpty());
  }

  @Test
  void defaultsWhenMetaXmlLocationIsIgnored(@TempDir Path dir) throws Exception {
    Path archive = archive(dir, "../secret.xml");
    Files.writeString(archive.resolve("eml.xml"), "<eml/>");

    assertEquals(Optional.of(archive.resolve("eml.xml")), MetadataPath.parsePath(archive));
  }
}
