package org.gbif.pipelines.ingest.options;

import com.google.common.base.Strings;
import java.io.File;
import java.util.Collections;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.ingest.utils.FsUtils;

/** Factory parsers arguments or file, registers and produces {@link PipelineOptions} */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PipelinesOptionsFactory {

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

  /** Register hdfs configs for options extended {@link InterpretationPipelineOptions} */
  public static void registerHdfs(InterpretationPipelineOptions options) {
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
  }

  /**
   * Creates {@link InterpretationPipelineOptions} from args
   *
   * @param args string arguments or file
   */
  public static InterpretationPipelineOptions createInterpretation(String[] args) {
    InterpretationPipelineOptions options = create(InterpretationPipelineOptions.class, args);
    registerHdfs(options);
    return options;
  }

  /**
   * Creates {@link EsIndexingPipelineOptions} from args
   *
   * @param args string arguments or file
   */
  public static EsIndexingPipelineOptions createIndexing(String[] args) {
    EsIndexingPipelineOptions options = create(EsIndexingPipelineOptions.class, args);
    registerHdfs(options);
    return options;
  }
}
