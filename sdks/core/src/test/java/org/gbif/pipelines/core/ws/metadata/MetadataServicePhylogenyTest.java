package org.gbif.pipelines.core.ws.metadata;

import java.io.IOException;
import java.util.List;
import okhttp3.ResponseBody;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.gbif.pipelines.core.ws.metadata.response.PhylogenyFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/**
 * Tests the phylogeny endpoints of {@link MetadataService}. The service is built directly because
 * {@link MetadataServiceFactory} is a JVM-wide singleton.
 */
public class MetadataServicePhylogenyTest {

  private static final String DATASET_KEY = "3a1ca64a-e5bd-49a1-abe1-9af509c1143d";

  private MockWebServer server;
  private MetadataService service;

  @Before
  public void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
    service =
        new Retrofit.Builder()
            .baseUrl(server.url("/"))
            .addConverterFactory(JacksonConverterFactory.create())
            .build()
            .create(MetadataService.class);
  }

  @After
  public void tearDown() throws IOException {
    server.shutdown();
  }

  @Test
  public void getPhylogenyFilesTest() throws Exception {
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/json")
            .setBody(
                "[{\"fileName\":\"global_tree_renamed.nex\",\"size\":1173495,\"format\":\"NEXUS\","
                    + "\"error\":\"failed to parse number\",\"trees\":[],"
                    + "\"treesTruncated\":false,\"isValid\":false},"
                    + "{\"fileName\":\"tree.nwk\",\"size\":10,\"format\":\"NEWICK\","
                    + "\"trees\":[],\"treesTruncated\":true,\"isValid\":true}]"));

    List<PhylogenyFile> files = service.getPhylogenyFiles(DATASET_KEY).execute().body();

    RecordedRequest request = server.takeRequest();
    Assert.assertEquals(
        "/v1/experimental/dataset/" + DATASET_KEY + "/phylogeny", request.getPath());

    Assert.assertNotNull(files);
    Assert.assertEquals(2, files.size());
    PhylogenyFile nexus = files.get(0);
    Assert.assertEquals("global_tree_renamed.nex", nexus.getFileName());
    Assert.assertEquals(Long.valueOf(1173495), nexus.getSize());
    Assert.assertEquals("NEXUS", nexus.getFormat());
    Assert.assertEquals("failed to parse number", nexus.getError());
    Assert.assertFalse(nexus.isValid());
    Assert.assertFalse(nexus.isTreesTruncated());
    Assert.assertTrue(files.get(1).isValid());
    Assert.assertTrue(files.get(1).isTreesTruncated());
  }

  @Test
  public void getPhylogenyFileTest() throws Exception {
    server.enqueue(new MockResponse().setBody("(A:0.1,B:0.2);"));

    try (ResponseBody body =
        service.getPhylogenyFile(DATASET_KEY, "my tree.nwk").execute().body()) {
      Assert.assertNotNull(body);
      Assert.assertEquals("(A:0.1,B:0.2);", body.string());
    }

    // the file name is path-encoded
    Assert.assertEquals(
        "/v1/experimental/dataset/" + DATASET_KEY + "/phylogeny/my%20tree.nwk",
        server.takeRequest().getPath());
  }

  @Test
  public void getMissingPhylogenyFileTest() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(404));

    Response<ResponseBody> response = service.getPhylogenyFile(DATASET_KEY, "nope.nex").execute();

    Assert.assertEquals(404, response.code());
  }
}
