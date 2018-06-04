package org.gbif.pipelines.labs;

import java.util.HashMap;
import java.util.Map;

import com.github.alexcojocaru.mojo.elasticsearch.v2.ItBase;
import com.github.alexcojocaru.mojo.elasticsearch.v2.client.ElasticsearchClientException;
import com.github.alexcojocaru.mojo.elasticsearch.v2.client.Monitor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Tests to verify partial updates approaches
 * Note: The test will not run on IDE, since pom.xml has maven plugin which starts elastic search before test begins.
 */
@RunWith(MockitoJUnitRunner.class)
public class AvroToESPipelineTest extends ItBase {

  private static String indexName = "interpreted-dataset_0645ccdb-e001-4ab0-9729-51f1755e007e_1";
  private final String[] args1 = {"--datasetId=0645ccdb-e001-4ab0-9729-51f1755e007e", "--attempt=1", "--defaultTargetDirectory=data/", "--ESAddresses=http://localhost:9200"};
  @Test
  public void testClusterRunning()
  {
    boolean isRunning = Monitor.isClusterRunning(clusterName, instanceCount, client);
    Assert.assertTrue("The ES cluster should be running", isRunning);
  }

  /**
   * verify the working of partial update approach1
   * @throws ElasticsearchClientException
   */
  @Test
  public void startJoinTest1() throws ElasticsearchClientException {
    AvroToESJoinPipeline pipeline = new AvroToESJoinPipeline();
    pipeline.main(args1);
    verifyResults();
  }

  /**
   * verify the working of partial update approach2
   * @throws ElasticsearchClientException
   */
  @Test
  public void startJoinTest2() throws ElasticsearchClientException {
    AvroToESJoinPipeline2 pipeline = new AvroToESJoinPipeline2();
    pipeline.main(args1);
    verifyResults();
  }

  public void verifyResults() throws ElasticsearchClientException {
    HashMap hashMap =
      client.get("/"+indexName+"/_search?pretty=true&q=*:*",
                 HashMap.class);
    Map hits = (Map)hashMap.get("hits");
    Assert.assertEquals(Integer.parseInt("7"),Integer.parseInt(hits.get("total").toString()));
  }

}
