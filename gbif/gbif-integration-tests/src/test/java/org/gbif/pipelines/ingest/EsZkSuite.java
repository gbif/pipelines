package org.gbif.pipelines.ingest;

import org.gbif.pipelines.ingest.pipelines.EventToEsIndexPipelineIT;
import org.gbif.pipelines.ingest.pipelines.HdfsViewPipelineIT;
import org.gbif.pipelines.ingest.pipelines.InterpretedToEsIndexExtendedPipelineIT;
import org.gbif.pipelines.ingest.pipelines.OccurrenceToEsIndexPipelineIT;
import org.gbif.pipelines.ingest.pipelines.VerbatimToEventPipelineIT;
import org.gbif.pipelines.ingest.pipelines.VerbatimToIdentifierPipelineIT;
import org.gbif.pipelines.ingest.pipelines.VerbatimToOccurrencePipelineIT;
import org.gbif.pipelines.ingest.utils.EsServer;
import org.gbif.pipelines.ingest.utils.ZkServer;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  // ZK and ES
  org.gbif.pipelines.ingest.java.pipelines.InterpretedToEsIndexExtendedPipelineIT.class,
  EventToEsIndexPipelineIT.class,
  InterpretedToEsIndexExtendedPipelineIT.class,
  OccurrenceToEsIndexPipelineIT.class,
  // ZK
  org.gbif.pipelines.ingest.java.pipelines.HdfsViewPipelineIT.class,
  org.gbif.pipelines.ingest.java.pipelines.VerbatimToOccurrencePipelineIT.class,
  HdfsViewPipelineIT.class,
  VerbatimToEventPipelineIT.class,
  VerbatimToIdentifierPipelineIT.class,
  VerbatimToOccurrencePipelineIT.class
})
public class EsZkSuite {
  @ClassRule public static final EsServer ES_SERVER = EsServer.getInstance();
  @ClassRule public static final ZkServer ZK_SERVER = ZkServer.getInstance();
}
