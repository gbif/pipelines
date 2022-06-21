package org.gbif.pipelines.diagnostics.migration;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.HbaseKeyMigrator;
import org.gbif.pipelines.keygen.api.KeyLookupResult;

@Slf4j
@Builder
public class IdentifiersFileMigrator {

  @NonNull private final String filePath;
  @Builder.Default private final String splitter = ",";
  @Builder.Default private final boolean deleteKeys = false;
  @Builder.Default private final boolean skipIssues = false;
  @NonNull private final String fromDatasetKey;
  @NonNull private final String toDatasetKey;

  @NonNull private final HBaseLockingKeyService keygenService;

  @SneakyThrows
  public void migrateIdentifiers() {

    try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), UTF_8)) {

      long lineCounter = 0;

      String line = reader.readLine();
      while (line != null) {

        lineCounter++;

        String[] split = line.trim().split(splitter);

        if (split.length == 2) {
          Optional<KeyLookupResult> migratedKey =
              HbaseKeyMigrator.builder()
                  .fromDatasetKey(fromDatasetKey)
                  .toDatasetKey(toDatasetKey)
                  .oldLookupKey(split[0])
                  .newLookupKey(split[1])
                  .keyService(keygenService)
                  .deleteKeys(deleteKeys)
                  .build()
                  .migrate();

          if (!skipIssues && !migratedKey.isPresent()) {
            log.warn("If you want to skip issues, please set skipIssues=true");
            break;
          }
        } else {
          log.warn(
              "Line: {}. There are {} line parameters, supported value is 2, try to change line splitter setting",
              lineCounter,
              split.length);
        }

        line = reader.readLine();
      }
      log.info("Finished. Read {} lines", lineCounter);
    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
    }
  }
}
