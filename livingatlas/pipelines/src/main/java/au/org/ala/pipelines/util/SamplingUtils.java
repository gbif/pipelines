package au.org.ala.pipelines.util;

import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.SamplingPipelineOptions;
import au.org.ala.sampling.SamplingService;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.ValidationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.utils.FsUtils;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

@Slf4j
public class SamplingUtils {

  public static SamplingService initSamplingService(String baseUrl) {
    ObjectMapper om =
        new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    JacksonConverterFactory jcf = JacksonConverterFactory.create(om);
    // set up sampling service
    Retrofit retrofit = new Retrofit.Builder().baseUrl(baseUrl).addConverterFactory(jcf).build();
    return retrofit.create(SamplingService.class);
  }

  public static Long samplingLastRan(SamplingPipelineOptions options, FileSystem fs)
      throws IOException {
    String samplingPath = getSampleAvroMetricPath(options);
    // hack for EMR
    if (samplingPath.startsWith("hdfs:///")) {
      samplingPath = samplingPath.substring(7);
    }
    Path metrics = new Path(samplingPath);

    if (fs.exists(metrics)) {
      log.info(
          "Sampling metrics at path {} accessible with last modified {}",
          samplingPath,
          fs.getFileStatus(metrics).getModificationTime());
      return fs.getFileStatus(metrics).getModificationTime();
    } else {
      log.info("Sampling metrics at path {} not accessible", samplingPath);
      log.info("Filesystem in use: " + fs);
      return -1L;
    }
  }

  public static void writeSamplingMetrics(
      SamplingPipelineOptions options, Integer counter, FileSystem fs) throws IOException {
    String samplingPath = getSampleAvroMetricPath(options);
    log.info("Writing metrics to {}", samplingPath);
    Yaml yaml = new Yaml();
    Map<String, Object> dataMap = new HashMap<>();
    dataMap.put("sampledRecords", counter);
    ALAFsUtils.deleteIfExist(fs, samplingPath);
    if (samplingPath.startsWith("hdfs:///")) {
      samplingPath = samplingPath.substring(7);
    }

    FsUtils.createFile(fs, samplingPath, yaml.dump(dataMap));
  }

  @NotNull
  public static String getSampleAvroMetricPath(AllDatasetsPipelinesOptions options) {

    if (options.getDatasetId() == null || "all".equals(options.getDatasetId())) {
      return options.getAllDatasetsInputPath() + "/" + ValidationUtils.SAMPLING_METRICS;
    }
    return options.getInputPath()
        + "/"
        + options.getDatasetId()
        + "/"
        + options.getAttempt()
        + "/"
        + ValidationUtils.SAMPLING_METRICS;
  }

  @NotNull
  public static String getSamplingDirectoryPath(AllDatasetsPipelinesOptions options) {
    if (options.getDatasetId() == null || "all".equals(options.getDatasetId())) {
      return options.getAllDatasetsInputPath() + "/sampling";
    }
    return String.join(
        "/",
        options.getInputPath(),
        options.getDatasetId(),
        options.getAttempt().toString(),
        "sampling");
  }
}
