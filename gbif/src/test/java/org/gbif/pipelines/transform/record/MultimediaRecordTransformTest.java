package org.gbif.pipelines.transform.record;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.utils.ExtendedRecordCustomBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.multimedia.MediaType;
import org.gbif.pipelines.io.avro.multimedia.Multimedia;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.transform.Kv2Value;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the {@link MultimediaRecordTransform}.
 */
@RunWith(JUnit4.class)
public class MultimediaRecordTransformTest {

  private static final String RECORD_ID = "123";
  private static final String URI =
    "http://specify-attachments-saiab.saiab.ac.za/originals/sp6-3853933608872243693.att.JPG";
  private static final String SOURCE = "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg";
  private static final String TITLE = "Geranium Plume Moth 0032";
  private static final String DESCRIPTION = "Geranium Plume Moth 0032 description";
  private static final String LICENSE = "BY-NC-SA 2.0";
  private static final String CREATOR = "Moayed Bahajjaj";
  private static final String CREATED = "2012-03-29";

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() {

    final List<ExtendedRecord> input = Collections.singletonList(createExtendedRecord());

    MultimediaRecordTransform multimediaTransform = MultimediaRecordTransform.create().withAvroCoders(p);

    PCollection<ExtendedRecord> inputStream = p.apply(Create.of(input));

    PCollectionTuple tuple = inputStream.apply(multimediaTransform);

    PCollection<MultimediaRecord> dataStream = tuple.get(multimediaTransform.getDataTag()).apply(Kv2Value.create());

    // Should
    PAssert.that(dataStream).containsInAnyOrder(createExpectedMultimedia());
    p.run();
  }

  private ExtendedRecord createExtendedRecord() {
    Map<String, String> audubonExtension = ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder()
      .accessURI(URI)
      .identifier("d79633d3-0967-40fa-9557-d6915e4d1353")
      .format("jpg")
      .title(TITLE)
      .description(DESCRIPTION)
      .derivedFrom(SOURCE)
      .license(LICENSE)
      .creator(CREATOR)
      .created(CREATED)
      .build();

    return ExtendedRecordCustomBuilder.create()
      .id(RECORD_ID)
      .addExtensionRecord(Extension.AUDUBON, audubonExtension)
      .build();
  }

  private MultimediaRecord createExpectedMultimedia() {

    Multimedia multimedia = Multimedia.newBuilder()
      .setIdentifier(URI)
      .setFormat("image/jpeg")
      .setTitle(TITLE)
      .setDescription(DESCRIPTION)
      .setLicense(LICENSE)
      .setCreator(CREATOR)
      .setCreated(CREATED)
      .setSource(SOURCE)
      .setType(MediaType.StillImage)
      .build();

    return MultimediaRecord.newBuilder()
      .setId(RECORD_ID)
      .setMultimediaItems(Collections.singletonList(multimedia))
      .build();
  }

}
