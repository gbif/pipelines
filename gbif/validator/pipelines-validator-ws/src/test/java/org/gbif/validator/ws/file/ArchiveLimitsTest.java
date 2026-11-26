package org.gbif.validator.ws.file;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.unit.DataSize;

class ArchiveLimitsTest {

  private static final ArchiveLimits LIMITS = new ArchiveLimits(DataSize.ofKilobytes(100), 3);

  private static Path zip(Path dir, int entries, int entrySize) throws Exception {
    Path zip = dir.resolve("test.zip");
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zip))) {
      for (int i = 0; i < entries; i++) {
        out.putNextEntry(new ZipEntry("file" + i + ".txt"));
        out.write(new byte[entrySize]);
        out.closeEntry();
      }
    }
    return zip;
  }

  private static Path gzip(Path dir, int size) throws Exception {
    Path gzip = dir.resolve("test.gz");
    try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(gzip))) {
      out.write(new byte[size]);
    }
    return gzip;
  }

  @Test
  void withinLimits(@TempDir Path dir) throws Exception {
    Path zip = zip(dir, 3, 1024);
    Path gzip = gzip(dir, 1024);
    assertDoesNotThrow(() -> LIMITS.check(zip, true));
    assertDoesNotThrow(() -> LIMITS.check(gzip, false));
  }

  @Test
  void unpackedSizeExceeded(@TempDir Path dir) throws Exception {
    Path zip = zip(dir, 1, 1024 * 1024);
    Path gzip = gzip(dir, 1024 * 1024);
    assertThrows(FileSizeException.class, () -> LIMITS.check(zip, true));
    assertThrows(FileSizeException.class, () -> LIMITS.check(gzip, false));
  }

  @Test
  void tooManyEntries(@TempDir Path dir) throws Exception {
    Path zip = zip(dir, 4, 1);
    assertThrows(FileSizeException.class, () -> LIMITS.check(zip, true));
  }
}
