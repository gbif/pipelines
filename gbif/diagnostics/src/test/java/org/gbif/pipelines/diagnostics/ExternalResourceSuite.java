package org.gbif.pipelines.diagnostics;

import org.gbif.pipelines.diagnostics.resources.HbaseServer;
import org.gbif.pipelines.diagnostics.strategy.BothStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.MaxStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.MinStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.OccurrenceIdStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.TripletStrategyIT;
import org.gbif.pipelines.diagnostics.tools.IdentifiersMigratorToolIT;
import org.gbif.pipelines.diagnostics.tools.RepairGbifIDLookupToolIT;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  BothStrategyIT.class,
  MaxStrategyIT.class,
  MinStrategyIT.class,
  OccurrenceIdStrategyIT.class,
  TripletStrategyIT.class,
  IdentifiersMigratorToolIT.class,
  RepairGbifIDLookupToolIT.class
})
public class ExternalResourceSuite {
  @ClassRule public static final HbaseServer HBASE_SERVER = HbaseServer.getInstance();
}
