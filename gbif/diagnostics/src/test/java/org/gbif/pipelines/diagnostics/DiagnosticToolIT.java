package org.gbif.pipelines.diagnostics;

import java.io.IOException;
import org.gbif.pipelines.diagnostics.common.HbaseServer;
import org.junit.Before;
import org.junit.ClassRule;

public class DiagnosticToolIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }
}
