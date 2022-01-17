package org.gbif.pipelines.validator.checklists;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.ArchiveFile;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LineCounter {

  public static long count(ArchiveFile arhiveFile) {
    return countLines(arhiveFile.getLocationFile(), areHeaderLinesIncluded(arhiveFile));
  }

  /** Exclude header from counter */
  private static boolean areHeaderLinesIncluded(ArchiveFile arhiveFile) {
    return arhiveFile.getIgnoreHeaderLines() != null && arhiveFile.getIgnoreHeaderLines() > 0;
  }

  /** Efficient way of counting lines */
  private static long countLines(File file, boolean areHeaderLinesIncluded) {
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
}
