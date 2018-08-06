package org.gbif.pipelines.transform;

import org.gbif.pipelines.config.base.BaseOptions;
import org.gbif.pipelines.GbifInterpretationType;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.utils.CodecUtils;
import org.gbif.pipelines.utils.FsUtils;

import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import static org.gbif.pipelines.utils.FsUtils.getRootPath;
import static org.gbif.pipelines.utils.FsUtils.getTempDir;

public class AvroOutputTransform {

  private static final String AVRO_EXT = ".avro";
  private final BaseOptions options;

  private AvroOutputTransform(BaseOptions options) {
    this.options = options;
  }

  public static AvroOutputTransform create(BaseOptions options) {
    return new AvroOutputTransform(options);
  }

  public <T> void write(
      PCollectionTuple tuple,
      Class<T> avroClass,
      RecordTransform<?, T> transform,
      GbifInterpretationType type) {

    if (options.getWriteOutput()) {
      String rootPath = FsUtils.buildPathString(getRootPath(options), type.name().toLowerCase());

      // write interpeted data
      String interpretedPath = FsUtils.buildPathString(rootPath, "interpreted");
      PCollection<T> data = tuple.get(transform.getDataTag()).apply(Values.create());
      write(data, avroClass, interpretedPath);

      // write issues
      String issuesPath = FsUtils.buildPathString(rootPath, "issues", "issues");
      PCollection<OccurrenceIssue> issues =
          tuple.get(transform.getIssueTag()).apply(Values.create());
      write(issues, OccurrenceIssue.class, issuesPath);
    }
  }

  public <T> void write(PCollection<T> collection, Class<T> avroClass, String path) {
    collection.apply("Write to avro " + path,
        AvroIO.write(avroClass)
            .to(FileSystems.matchNewResource(path, false))
            .withSuffix(AVRO_EXT)
            .withCodec(CodecUtils.parseAvroCodec(options.getAvroCompressionType()))
            .withTempDirectory(FileSystems.matchNewResource(getTempDir(options), true)));
  }
}
