package org.gbif.pipelines.factory;

import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.junit.Assert;
import org.junit.Test;

public class MetadataServiceClientFactoryTest {

  @Test
  public void sameLinkToObjectTest() {

    // State
    PipelinesConfig pc = new PipelinesConfig();
    WsConfig wc = new WsConfig();
    wc.setWsUrl("https://www.gbif.org/");
    pc.setGbifApi(wc);

    // When
    SerializableSupplier<MetadataServiceClient> supplierOne =
        MetadataServiceClientFactory.getInstanceSupplier(pc);
    SerializableSupplier<MetadataServiceClient> supplierTwo =
        MetadataServiceClientFactory.getInstanceSupplier(pc);

    // Should
    Assert.assertSame(supplierOne.get(), supplierTwo.get());
  }

  @Test
  public void newObjectTest() {
    // State
    PipelinesConfig pc = new PipelinesConfig();
    WsConfig wc = new WsConfig();
    wc.setWsUrl("https://www.gbif.org/");
    pc.setGbifApi(wc);

    // When
    SerializableSupplier<MetadataServiceClient> supplierOne =
            MetadataServiceClientFactory.createSupplier(pc);
    SerializableSupplier<MetadataServiceClient> supplierTwo =
            MetadataServiceClientFactory.createSupplier(pc);

    // Should
    Assert.assertNotSame(supplierOne.get(), supplierTwo.get());
  }
}
