package org.gbif.pipelines;

import org.gbif.pipelines.core.io.ElasticsearchWriterIT;
import org.gbif.pipelines.estools.service.EsIndexIT;
import org.gbif.pipelines.estools.service.EsServiceIT;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ElasticsearchWriterIT.class, EsIndexIT.class, EsServiceIT.class})
public class EsSuite {
  @ClassRule public static final EsServer ES_SERVER = EsServer.getInstance();
}
