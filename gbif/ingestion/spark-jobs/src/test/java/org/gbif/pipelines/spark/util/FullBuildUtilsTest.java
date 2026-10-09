package org.gbif.pipelines.spark.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FullBuildUtilsTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private FileSystem fileSystem;
  private PipelinesConfig config;

  @Before
  public void setUp() throws Exception {
    fileSystem = FileSystem.getLocal(new Configuration());
    config = new PipelinesConfig();
    config.setOutputPath(folder.getRoot().getAbsolutePath());
  }

  @Test
  public void latestSuccessfulAttemptIsTheNewestSuccess() throws Exception {
    success("d1", "2", "json", 1_000L);
    success("d1", "10", "json", 2_000L);
    success("d1", "11", "json", 500L);
    // not successful
    new File(folder.getRoot(), "d1/12/json").mkdirs();

    assertEquals(
        Optional.of(10), FullBuildUtils.latestSuccessfulAttempt(fileSystem, config, "d1", "json"));
  }

  @Test
  public void latestSuccessfulAttemptOfTheSourceDirectory() throws Exception {
    success("d1", "1", "json", 1_000L);
    success("d1", "2", "event_json", 2_000L);

    assertEquals(
        Optional.of(1), FullBuildUtils.latestSuccessfulAttempt(fileSystem, config, "d1", "json"));
    assertEquals(
        Optional.of(2),
        FullBuildUtils.latestSuccessfulAttempt(fileSystem, config, "d1", "event_json"));
  }

  @Test
  public void noSuccessfulAttempt() throws Exception {
    new File(folder.getRoot(), "d1/1/json").mkdirs();

    assertEquals(
        Optional.empty(), FullBuildUtils.latestSuccessfulAttempt(fileSystem, config, "d1", "json"));
    assertEquals(
        Optional.empty(),
        FullBuildUtils.latestSuccessfulAttempt(fileSystem, config, "missing", "json"));
  }

  private void success(String datasetKey, String attempt, String sourceDirectory, long mtime)
      throws Exception {
    Path success =
        new Path(
            folder.getRoot().getAbsolutePath()
                + "/"
                + datasetKey
                + "/"
                + attempt
                + "/"
                + sourceDirectory
                + "/_SUCCESS");
    fileSystem.create(success).close();
    fileSystem.setTimes(success, mtime, -1);
  }
}
