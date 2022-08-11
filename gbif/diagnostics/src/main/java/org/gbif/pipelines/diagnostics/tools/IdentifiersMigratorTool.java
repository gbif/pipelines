package org.gbif.pipelines.diagnostics.tools;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.beust.jcommander.Parameter;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.pipelines.diagnostics.common.KeygenServiceFactory;
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

  @Parameter(names = "--from-dataset")
  @NotNull
  public String fromDatasetKey;

  @Parameter(names = "--to-dataset")
  @NotNull
  public String toDatasetKey;

  @Parameter(names = "--zk-connection", description = "Zookeeper connection")
  public String zkConnection;

  @Parameter(names = "--lookup-table", description = "Hbase occurrence lookup table")
  @NotNull
  public String lookupTable;

  @Parameter(names = "--counter-table", description = "Hbase counter lookup table")
  @NotNull
  public String counterTable;

  @Parameter(names = "--occurrence-table", description = "Hbase occurrence table")
  @NotNull
  public String occurrenceTable;

  @Parameter(names = "--help", description = "Display help information", order = 4)
  @Builder.Default
  public boolean help = false;

  @Builder.Default public Connection connection = null;

  @Override
  public boolean getHelp() {
    return help;
  }

  @Override
  public void run() {

    HBaseLockingKeyService keygenService = null;

    try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), UTF_8)) {

      keygenService =
          KeygenServiceFactory.builder()
              .zkConnection(zkConnection)
              .lookupTable(lookupTable)
              .counterTable(counterTable)
              .occurrenceTable(occurrenceTable)
              .connection(connection)
              .build()
              .create();

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
    } finally {
      if (keygenService != null && connection == null) {
        keygenService.close();
      }
    }
  }
}
