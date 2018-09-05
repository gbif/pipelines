package org.gbif.pipelines.base.options;

import java.io.File;
import java.util.Collections;

import com.google.common.base.Strings;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** TODO: DOC */
public final class PipelinesOptionsFactory {

  private PipelinesOptionsFactory() {}

  public static InterpretationPipelineOptions create(String[] args) {
    return create(InterpretationPipelineOptions.class, args);
  }

  public static IndexingPipelineOptions createIndexing(String[] args) {
    return create(IndexingPipelineOptions.class, args);
  }

  private static <T extends InterpretationPipelineOptions> T create(Class<T> clazz, String[] args) {
    PipelineOptionsFactory.register(clazz);
    T options = PipelineOptionsFactory.fromArgs(args).withValidation().as(clazz);

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
}
