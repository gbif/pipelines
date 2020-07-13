package au.org.ala.pipelines.transforms;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;

import java.io.Serializable;

@AllArgsConstructor(staticName = "create")
public class ALACSVDocumentTransform implements Serializable {

  private static final long serialVersionUID = 1279313931024806169L;
  // Core
  @NonNull
  private final TupleTag<LocationRecord> lrTag;

  public ParDo.SingleOutput<KV<String, CoGbkResult>, String> converter() {

    DoFn<KV<String, CoGbkResult>, String> fn = new DoFn<KV<String, CoGbkResult>, String>() {

      @ProcessElement
      public void processElement(ProcessContext c) {
        CoGbkResult v = c.element().getValue();
        String k = c.element().getKey();
        // Core
        LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
        c.output(lr.getDecimalLatitude() + "," + lr.getDecimalLongitude());
      }
    };

    return ParDo.of(fn);
  }
}
