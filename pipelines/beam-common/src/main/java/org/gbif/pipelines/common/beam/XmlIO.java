package org.gbif.pipelines.common.beam;

import java.io.InputStream;
import java.nio.channels.Channels;

import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ExtendedRecordConverter;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.SneakyThrows;

/**
 * Composite transformation, IO operation for XML archives formats.
 *
 * <p>Provides the ability to read XML files as a bounded source. The transformation can read as a file or files by
 * matching
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
   * Path to a file or files:
   * "/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/response.00002.xml"
   * "/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/*.xml"
   * "/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/*"
   */
  private final String path;

  @Override
  public PCollection<ExtendedRecord> expand(PBegin input) {

    SingleOutput<String, ResourceId> pathToResourceId = ParDo.of(new DoFn<String, ResourceId>() {
      @SneakyThrows
      @ProcessElement
      public void processElement(@Element String path, OutputReceiver<ResourceId> out) {
        MatchResult match = FileSystems.match(path);
        match.metadata().forEach(x -> out.output(x.resourceId()));
      }
    });

    SingleOutput<ResourceId, RawXmlOccurrence> resourceIdToRawXml = ParDo.of(
        new DoFn<ResourceId, RawXmlOccurrence>() {
          @SneakyThrows
          @ProcessElement
          public void processElement(@Element ResourceId resourceId, OutputReceiver<RawXmlOccurrence> out) {
            @Cleanup InputStream is = Channels.newInputStream(FileSystems.open(resourceId));
            new OccurrenceParser().parseStream(is).forEach(out::output);
          }
        });

    SingleOutput<RawXmlOccurrence, RawOccurrenceRecord> rawXmlToRawOcc = ParDo.of(
        new DoFn<RawXmlOccurrence, RawOccurrenceRecord>() {
          @ProcessElement
          public void processElement(@Element RawXmlOccurrence rxo, OutputReceiver<RawOccurrenceRecord> out) {
            XmlFragmentParser.parseRecord(rxo).forEach(out::output);
          }
        });

    SingleOutput<RawOccurrenceRecord, ExtendedRecord> rawOccToExtRec = ParDo.of(
        new DoFn<RawOccurrenceRecord, ExtendedRecord>() {
          @ProcessElement
          public void processElement(@Element RawOccurrenceRecord roc, OutputReceiver<ExtendedRecord> out) {
            out.output(ExtendedRecordConverter.from(roc));
          }
        });

    return input
        .apply("Input folder path", Create.of(path))
        .apply("Read files paths", pathToResourceId)
        .apply("ResourceId To RawXmlOccurrence", resourceIdToRawXml)
        .apply("RawXmlOccurrence To RawOccurrenceRecord", rawXmlToRawOcc)
        .apply("RawOccurrenceRecord To ExtendedRecord", rawOccToExtRec);
  }

}
