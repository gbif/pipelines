package org.gbif.pipelines.diagnostics;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.beust.jcommander.Parameter;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.HbaseKeyMigrator;
import org.gbif.pipelines.keygen.api.KeyLookupResult;

@Slf4j
@Builder
public class IdentifiersMigratorTool implements Tool {

  @Parameter(names = "--tool")
  public CliTool tool;

  @Parameter(names = "--file-path")
  @NotNull
  public String filePath;

  @Parameter(names = "--splitter")
  @Builder.Default
  public String splitter = ",";

  @Parameter(names = "--delete-keys")
  @Builder.Default
  public boolean deleteKeys = false;

  @Parameter(names = "--skip-issues")
  @Builder.Default
  public boolean skipIssues = false;

  @Parameter(names = "--from-dataset-key")
  @NotNull
  public String fromDatasetKey;

  @Parameter(names = "--to-dataset-key")
  @NotNull
  public String toDatasetKey;

  @NonNull public HBaseLockingKeyService keygenService;

  @Override
  public void run() {

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
