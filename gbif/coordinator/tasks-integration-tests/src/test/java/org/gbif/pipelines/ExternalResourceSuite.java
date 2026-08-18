package org.gbif.pipelines;

import org.gbif.pipelines.tasks.PipelinesCallbackIT;
import org.gbif.pipelines.tasks.resources.EsServer;
import org.gbif.pipelines.tasks.resources.ZkServer;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorCallbackIT;
import org.gbif.pipelines.tasks.verbatims.abcd.AbcdToAvroCallbackIT;
import org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroCallbackIT;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroCallbackIT;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  ArchiveValidatorCallbackIT.class,
  AbcdToAvroCallbackIT.class,
  DwcaToAvroCallbackIT.class,
  XmlToAvroCallbackIT.class,
  PipelinesCallbackIT.class
})
public class ExternalResourceSuite {
  @ClassRule public static final EsServer ES_SERVER = EsServer.getInstance();
  @ClassRule public static final ZkServer ZK_SERVER_SERVER = ZkServer.getInstance();
}
