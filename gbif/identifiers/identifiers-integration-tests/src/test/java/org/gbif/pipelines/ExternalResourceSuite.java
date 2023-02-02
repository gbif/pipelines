package org.gbif.pipelines;

import org.gbif.pipelines.diagnostics.strategy.BothStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.MaxStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.MinStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.OccurrenceIdStrategyIT;
import org.gbif.pipelines.diagnostics.strategy.TripletStrategyIT;
import org.gbif.pipelines.diagnostics.tools.IdentifiersMigratorToolIT;
import org.gbif.pipelines.diagnostics.tools.RepairGbifIDLookupToolIT;
import org.gbif.pipelines.keygen.HBaseLockingKeyServiceIT;
import org.gbif.pipelines.keygen.HbaseKeyMigratorIT;
import org.gbif.pipelines.keygen.KeygenIT;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactoryIT;
import org.gbif.pipelines.resources.HbaseServer;
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
  RepairGbifIDLookupToolIT.class,
  HbaseKeyMigratorIT.class,
  HBaseLockingKeyServiceIT.class,
  KeygenIT.class,
  HbaseConnectionFactoryIT.class
})
public class ExternalResourceSuite {
  @ClassRule public static final HbaseServer HBASE_SERVER = HbaseServer.getInstance();
}
