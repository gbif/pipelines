package org.gbif.pipelines.keygen;

import org.gbif.pipelines.keygen.common.HbaseConnectionFactoryIT;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  HbaseKeyMigratorIT.class,
  HBaseLockingKeyServiceIT.class,
  KeygenIT.class,
  HbaseConnectionFactoryIT.class
})
public class HbaseSuite {
  @ClassRule public static final HbaseServer HBASE_SERVER = HbaseServer.getInstance();
}
