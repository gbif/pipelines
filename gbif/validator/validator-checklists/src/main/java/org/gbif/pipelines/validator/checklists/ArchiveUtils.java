package org.gbif.pipelines.validator.checklists;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.ArchiveFile;

@UtilityClass
@Slf4j
public class ArchiveUtils {

  /** Efficient way of counting lines. */
  public static long countLines(File file, boolean areHeaderLinesIncluded) {
    long lines = areHeaderLinesIncluded ? -1 : 0;
    try (BufferedReader reader = Files.newBufferedReader(file.toPath(), UTF_8)) {
      while (reader.readLine() != null) {
        lines++;
      }
    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
    }
    return lines;
  }

  /** Exclude header from counter. */
  public static boolean areHeaderLinesIncluded(ArchiveFile archiveFile) {
    return archiveFile.getIgnoreHeaderLines() != null && archiveFile.getIgnoreHeaderLines() > 0;
  }
}
