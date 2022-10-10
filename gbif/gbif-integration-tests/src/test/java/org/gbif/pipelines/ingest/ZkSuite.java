package org.gbif.pipelines.ingest;

import org.gbif.pipelines.ingest.pipelines.HdfsViewPipelineIT;
import org.gbif.pipelines.ingest.pipelines.VerbatimToEventPipelineIT;
import org.gbif.pipelines.ingest.pipelines.VerbatimToIdentifierPipelineIT;
import org.gbif.pipelines.ingest.pipelines.VerbatimToOccurrencePipelineIT;
import org.gbif.pipelines.ingest.utils.ZkServer;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  org.gbif.pipelines.ingest.java.pipelines.HdfsViewPipelineIT.class,
  org.gbif.pipelines.ingest.java.pipelines.VerbatimToOccurrencePipelineIT.class,
  HdfsViewPipelineIT.class,
  VerbatimToEventPipelineIT.class,
  VerbatimToIdentifierPipelineIT.class,
  VerbatimToOccurrencePipelineIT.class
})
public class ZkSuite {
  @ClassRule public static final ZkServer ZK_SERVER = ZkServer.getInstance();
}
