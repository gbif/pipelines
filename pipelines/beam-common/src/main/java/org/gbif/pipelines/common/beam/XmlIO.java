package org.gbif.pipelines.common.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.ARCHIVE_TO_ER_COUNT;

import java.io.InputStream;
import java.nio.channels.Channels;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ExtendedRecordConverter;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Composite transformation, IO operation for XML archives formats.
 *
 * <p>Provides the ability to read XML files as a bounded source. The transformation can read as a
 * file or files by matching
 *
 * <p>To use this:
 *
 * <pre>{@code
 * p.apply("read", XmlIO.read("/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/response.00002.xml");
 *     ...;
 *
 * or
 *
 * p.apply("read", XmlIO.read("/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/*.xml");
 *     ...;
 * or
 *
 * p.apply("read", XmlIO.read("/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/*");
 *    ...;
 * }</pre>
 */
@AllArgsConstructor(staticName = "read")
public class XmlIO extends PTransform<PBegin, PCollection<ExtendedRecord>> {

  /**
   * Path to a file or files: "/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/response.00002.xml"
   * "/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/*.xml" "/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/*"
   */
  private final String path;

  @Override
  public PCollection<ExtendedRecord> expand(PBegin input) {

    SingleOutput<String, ResourceId> pathToResourceId =
        ParDo.of(
            new DoFn<String, ResourceId>() {
              @SneakyThrows
              @ProcessElement
              public void processElement(@Element String path, OutputReceiver<ResourceId> out) {
                MatchResult match = FileSystems.match(path);
                match.metadata().forEach(x -> out.output(x.resourceId()));
              }
            });

    SingleOutput<ResourceId, ExtendedRecord> resourceIdToExtRec =
        ParDo.of(
            new DoFn<ResourceId, ExtendedRecord>() {

              private final Counter xmlCount = Metrics.counter("XmlIO", ARCHIVE_TO_ER_COUNT);

              @SneakyThrows
              @ProcessElement
              public void processElement(
                  @Element ResourceId resourceId, OutputReceiver<ExtendedRecord> out) {
                try (InputStream is = Channels.newInputStream(FileSystems.open(resourceId))) {
                  new OccurrenceParser()
                      .parseStream(is)
                      .forEach(
                          rxo ->
                              XmlFragmentParser.parseRecord(rxo).stream()
                                  .map(ExtendedRecordConverter::from)
                                  .forEach(
                                      er -> {
                                        xmlCount.inc();
                                        out.output(er);
                                      }));
                }
              }
            });

    return input
        .apply("Input folder path", Create.of(path))
        .apply("Read files paths", pathToResourceId)
        .apply("ResourceId To ExtendedRecord", resourceIdToExtRec);
  }
}
