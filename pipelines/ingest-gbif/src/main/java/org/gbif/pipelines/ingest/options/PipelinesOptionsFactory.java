package org.gbif.pipelines.ingest.options;

import org.gbif.pipelines.ingest.utils.FsUtils;

import java.io.File;
import java.util.Collections;

import com.google.common.base.Strings;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** Factory parsres arguments or file, registers and produces {@link PipelineOptions} */
public final class PipelinesOptionsFactory {

  private PipelinesOptionsFactory() {}

  /**
   * Creates pipeline options object extended {@link PipelineOptions} from args or file
   *
   * @param clazz class extended {@link PipelineOptions}
   * @param args string arguments or file
   */
  public static <T extends PipelineOptions> T create(Class<T> clazz, String[] args) {
    String[] parsedArgs = FsUtils.readArgsAsFile(args);
    PipelineOptionsFactory.register(clazz);
    return PipelineOptionsFactory.fromArgs(parsedArgs).withValidation().as(clazz);
  }

  /**
   * Creates pipeline options object extended {@link InterpretationPipelineOptions} from args or
   * file, adds hdfs configuration to options
   *
   * @param clazz class extended {@link InterpretationPipelineOptions}
   * @param args string arguments or file
   */
  private static <T extends InterpretationPipelineOptions> T createWithHdfs(
      Class<T> clazz, String[] args) {
    T options = create(clazz, args);

    String hdfsPath = options.getHdfsSiteConfig();
    String corePath = options.getCoreSiteConfig();
    boolean isHdfsExist = !Strings.isNullOrEmpty(hdfsPath) && new File(hdfsPath).exists();
    boolean isCoreExist = !Strings.isNullOrEmpty(corePath) && new File(corePath).exists();
    if (isHdfsExist && isCoreExist) {
      Configuration conf = new Configuration(false);
      conf.addResource(new Path(hdfsPath));
      conf.addResource(new Path(corePath));
      options.setHdfsConfiguration(Collections.singletonList(conf));
    }

    return options;
  }

  /**
   * Creates {@link InterpretationPipelineOptions} from args
   *
   * @param args string arguments or file
   */
  public static InterpretationPipelineOptions createInterpretation(String[] args) {
    return createWithHdfs(InterpretationPipelineOptions.class, args);
  }

  /**
   * Creates {@link EsIndexingPipelineOptions} from args
   *
   * @param args string arguments or file
   */
  public static EsIndexingPipelineOptions createIndexing(String[] args) {
    return createWithHdfs(EsIndexingPipelineOptions.class, args);
  }
}
