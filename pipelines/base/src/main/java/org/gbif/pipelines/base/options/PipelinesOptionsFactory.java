package org.gbif.pipelines.base.options;

import org.gbif.pipelines.base.utils.FsUtils;

import java.io.File;
import java.util.Collections;

import com.google.common.base.Strings;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** TODO: DOC */
public final class PipelinesOptionsFactory {

  private PipelinesOptionsFactory() {}

  public static <T extends PipelineOptions> T create(Class<T> clazz, String[] args) {
    PipelineOptionsFactory.register(clazz);
    String[] parsedArgs = FsUtils.readArgsAsFile(args);
    return PipelineOptionsFactory.fromArgs(parsedArgs).withValidation().as(clazz);
  }

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

  public static InterpretationPipelineOptions createInterpretation(String[] args) {
    return createWithHdfs(InterpretationPipelineOptions.class, args);
  }

  public static IndexingPipelineOptions createIndexing(String[] args) {
    return createWithHdfs(IndexingPipelineOptions.class, args);
  }
}
