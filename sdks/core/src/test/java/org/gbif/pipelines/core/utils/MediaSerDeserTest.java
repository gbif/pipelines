package org.gbif.pipelines.core.utils;

import java.util.List;
import org.gbif.pipelines.io.avro.Humboldt;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonHumboldtRecord;
import org.junit.Test;

public class MediaSerDeserTest {

  @Test
  public void humboldtItemsTest() {
    Humboldt humboldt =
        Humboldt.newBuilder()
            .setTargetTaxonomicScope(
                List.of(
                    TaxonHumboldtRecord.newBuilder()
                        .setClassification(
                            List.of(
                                RankedName.newBuilder().setRank("rank").setName("name").build()))
                        .build()))
            .build();
    MediaSerDeser.humboldtToJson(List.of(humboldt));
  }
}
