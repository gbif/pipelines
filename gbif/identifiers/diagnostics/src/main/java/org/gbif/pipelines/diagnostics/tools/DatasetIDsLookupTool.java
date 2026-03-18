package org.gbif.pipelines.diagnostics.tools;

import com.beust.jcommander.Parameter;
import jakarta.validation.constraints.NotNull;
import java.util.function.ObjLongConsumer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.pipelines.diagnostics.common.KeygenServiceFactory;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;

@Slf4j
@Builder
public class DatasetIDsLookupTool implements Tool {

  @Parameter(names = "--tool")
  public CliTool tool;

  @Parameter(names = "--dataset", description = "Registry dataset key")
  @NotNull
  public String datasetKey;

  @Parameter(names = "--limit", description = "Output size limit")
  public Long limit;

  @Parameter(names = "--zk-connection", description = "Zookeeper connection")
  public String zkConnection;

  @Parameter(names = "--hbase-znode", description = "Hbase zookeeper node name")
  public String hbaseZnode;

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

    try {

      keygenService =
          KeygenServiceFactory.builder()
              .zkConnection(zkConnection)
              .hbaseZnode(hbaseZnode)
              .lookupTable(lookupTable)
              .counterTable(counterTable)
              .occurrenceTable(occurrenceTable)
              .connection(connection)
              .datasetKey(datasetKey)
              .build()
              .create();

      ObjLongConsumer<String> fn = (key, value) -> log.info("Key: {}, GBIF ID: {}", key, value);
      keygenService.findKeysByScope(datasetKey, fn, limit);

    } finally {
      if (keygenService != null && connection == null) {
        keygenService.close();
      }
    }
  }
}
