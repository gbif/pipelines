package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Level;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OrchestratedArchiveValidationTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test
  public void aFatalIssueMakesTheArchiveInvalidWithItsMessage() {
    FileInfo eml =
        FileInfo.builder()
            .fileName("eml.xml")
            .issues(
                List.of(
                    IssueInfo.create(
                        EvaluationType.EML_NOT_FOUND, Level.FATAL.name(), "metadata missing")))
            .build();
    OrchestratedArchiveValidation result =
        new OrchestratedArchiveValidation(List.of(eml), Map.of());

    assertFalse(result.valid());
    assertEquals("EML_NOT_FOUND: metadata missing", result.firstFatal().orElseThrow());
    assertEquals(false, result.report().get("valid"));
    assertEquals("EML_NOT_FOUND: metadata missing", result.report().get("invalidationReason"));
  }

  @Test
  public void warningsKeepTheArchiveValid() {
    FileInfo core =
        FileInfo.builder()
            .fileName("occurrence.txt")
            .issues(List.of(IssueInfo.create(EvaluationType.EML_GBIF_SCHEMA, "WARNING", "x")))
            .build();

    assertTrue(new OrchestratedArchiveValidation(List.of(core), Map.of()).valid());
  }

  @Test
  public void theUploadIsTheOneArchiveInTheValidationFolder() throws Exception {
    Path folder = tmp.newFolder().toPath();
    Files.writeString(folder.resolve("taxon.txt"), "taxonID\n1\n");
    Files.writeString(folder.resolve("meta.xml"), "<archive/>");
    Path upload = folder.resolve("Cats checklist.zip");
    try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(upload))) {
      zip.putNextEntry(new ZipEntry("taxon.txt"));
      zip.write("taxonID\n1\n".getBytes());
      zip.closeEntry();
    }

    assertEquals(upload, UploadedArchive.find(folder));
  }

  @Test
  public void gzipUploadsAreFoundAndTwoArchivesAreRefused() throws Exception {
    Path folder = tmp.newFolder().toPath();
    Path gz = folder.resolve("upload.tar.gz");
    try (GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(gz))) {
      out.write("data".getBytes());
    }
    assertEquals(gz, UploadedArchive.find(folder));

    try (ZipOutputStream zip =
        new ZipOutputStream(Files.newOutputStream(folder.resolve("b.zip")))) {
      zip.putNextEntry(new ZipEntry("x"));
      zip.closeEntry();
    }
    assertThrows(IllegalStateException.class, () -> UploadedArchive.find(folder));
  }
}
