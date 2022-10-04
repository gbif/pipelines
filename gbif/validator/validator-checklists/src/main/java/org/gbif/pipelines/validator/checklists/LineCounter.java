package org.gbif.pipelines.validator.checklists;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.ArchiveFile;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LineCounter {

  /** Efficient way of counting lines */
  public static long count(ArchiveFile arhiveFile) {
    long lines = 0;
    long header = Optional.ofNullable(arhiveFile.getIgnoreHeaderLines()).orElse(0);
    for (File file : arhiveFile.getLocationFiles()) {
      try (BufferedReader reader = Files.newBufferedReader(file.toPath(), UTF_8)) {
        while (reader.readLine() != null) {
          lines++;
        }
        lines = lines - header;
      } catch (IOException ex) {
        log.error(ex.getMessage(), ex);
      }
    }
    return lines;
  }
}
