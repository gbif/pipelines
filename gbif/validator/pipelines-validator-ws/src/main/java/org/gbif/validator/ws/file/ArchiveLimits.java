package org.gbif.validator.ws.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import lombok.Getter;
import org.springframework.util.unit.DataSize;

/** Limits on what a compressed file unpacks to, checked before it is decompressed. */
@Getter
public class ArchiveLimits {

  public static final ArchiveLimits DEFAULT = new ArchiveLimits(DataSize.ofGigabytes(20), 10_000);

  private final long maxUnpackedSize;
  private final int maxEntries;

  public ArchiveLimits(DataSize maxUnpackedSize, int maxEntries) {
    this.maxUnpackedSize = maxUnpackedSize.toBytes();
    this.maxEntries = maxEntries;
  }

  /** Reads the zip or gzip file, failing as soon as it unpacks beyond the limits. */
  public void check(Path file, boolean zip) throws IOException {
    if (zip) {
      checkZip(file);
    } else {
      try (InputStream in = new GZIPInputStream(Files.newInputStream(file))) {
        count(in, 0);
      }
    }
  }

  private void checkZip(Path file) throws IOException {
    try (ZipFile zipFile = new ZipFile(file.toFile())) {
      if (zipFile.size() > maxEntries) {
        throw new FileSizeException("Archive has more than " + maxEntries + " entries");
      }
      long total = 0;
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (!entry.isDirectory()) {
          try (InputStream in = zipFile.getInputStream(entry)) {
            total = count(in, total);
          }
        }
      }
    }
  }

  private long count(InputStream in, long total) throws IOException {
    byte[] buffer = new byte[64 * 1024];
    int read;
    while ((read = in.read(buffer)) != -1) {
      total += read;
      if (total > maxUnpackedSize) {
        throw new FileSizeException(
            "Archive unpacks to more than "
                + DataSize.ofBytes(maxUnpackedSize).toMegabytes()
                + "MB");
      }
    }
    return total;
  }
}
