package org.gbif.pipelines.minipipelines.dwca;

import org.gbif.pipelines.assembling.GbifInterpretationType;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.transform.RecordTransform;
import org.gbif.pipelines.utils.FsUtils;

import com.google.common.base.Strings;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import static org.gbif.pipelines.minipipelines.dwca.DwcaPipelineOptions.PipelineStep.INTERPRET;

class OutputWriter {

  private static final String TEMP_DEFAULT = "tmp";

  OutputWriter() {}

  static <T> void writeToAvro(
      PCollection<T> records, Class<T> avroClass, DwcaPipelineOptions options, String path) {
    records.apply(
        AvroIO.write(avroClass)
            .to(FileSystems.matchNewResource(path, false))
            .withSuffix(".avro")
            .withCodec(CodecFactory.snappyCodec())
            .withTempDirectory(FileSystems.matchNewResource(getTempDir(options), true)));
  }

  static <T> void writeInterpretationResult(
      PCollectionTuple tuple,
      Class<T> avroClass,
      RecordTransform<ExtendedRecord, T> transform,
      DwcaPipelineOptions options,
      GbifInterpretationType type) {

    // only write if it'' the final the step or the intermediate outputs are not ignored
    if (INTERPRET == options.getPipelineStep() || !options.getIgnoreIntermediateOutputs()) {

      String rootPath = FsUtils.buildPathString(getRootPath(options), type.name().toLowerCase());

      // write interpeted data
      String interpretedPath = FsUtils.buildPathString(rootPath, "interpreted");
      PCollection<T> data = tuple.get(transform.getDataTag()).apply(Values.create());
      writeToAvro(data, avroClass, options, interpretedPath);

      // write issues
      String issuesPath = FsUtils.buildPathString(rootPath, "issues");
      PCollection<OccurrenceIssue> issues = tuple.get(transform.getIssueTag()).apply(Values.create());
      writeToAvro(issues, OccurrenceIssue.class, options, issuesPath);
    }
  }

  static String getRootPath(DwcaPipelineOptions options) {
    return FsUtils.buildPathString(
        options.getTargetPath(), options.getDatasetId(), String.valueOf(options.getAttempt()));
  }

  static String getTempDir(DwcaPipelineOptions options) {
    return Strings.isNullOrEmpty(options.getTempLocation())
        ? FsUtils.buildPathString(options.getTargetPath(), TEMP_DEFAULT)
        : options.getTempLocation();
  }
}
