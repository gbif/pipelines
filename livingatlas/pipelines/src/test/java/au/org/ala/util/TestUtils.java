package au.org.ala.util;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import java.io.File;
import java.net.URL;
import lombok.extern.slf4j.Slf4j;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import okio.Okio;
import okio.Source;
import org.apache.commons.io.FileUtils;

@Slf4j
public class TestUtils {

  public static ALAPipelinesConfig getConfig() {
    String absolutePath = new File(getPipelinesConfigFile()).getAbsolutePath();
    return ALAPipelinesConfigFactory.getInstance(null, null, absolutePath).get();
  }

  public static String getPipelinesConfigFile() {
    return System.getProperty("pipelinesTestYamlConfigFile", "src/test/resources/pipelines.yaml");
  }

  public static int getCollectoryPort() throws Exception {
    String urlStr = TestUtils.getConfig().getCollectory().getWsUrl();
    URL url = new URL(urlStr);
    return url.getPort();
  }

  public static int getSpeciesListPort() throws Exception {
    String urlStr = TestUtils.getConfig().getSpeciesListService().getWsUrl();
    URL url = new URL(urlStr);
    return url.getPort();
  }

  public static MockWebServer createMockSpeciesLists(){
    MockWebServer server = new MockWebServer();
    final Dispatcher dispatcher =
        new Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

            try {
              // authoritative lists
              if (request.getPath().startsWith("/ws/speciesList")) {
                String responseBody =
                    FileUtils.readFileToString(new File("src/test/resources/species-lists/list.json"), "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
              }

              // list download
              if (request.getPath().startsWith("/speciesListItem/downloadList")) {

                Source source = Okio.source(new File("src/test/resources/species-lists/test-list.csv"));
                Buffer buffer = new Buffer();
                buffer.writeAll(source);
                return new MockResponse()
                        .setHeader("contentType", "text/csv")
                        .setHeader("Content-Disposition", "attachment;filename=test-list.csv")
                        .setResponseCode(200)
                        .setBody(buffer);
              }
            } catch (Exception e){
              return new MockResponse().setResponseCode(500);
            }
            return new MockResponse().setResponseCode(400);
          }
        };
    server.setDispatcher(dispatcher);
    return server;
  }


  public static MockWebServer createMockCollectory() {
    MockWebServer server = new MockWebServer();
    final Dispatcher dispatcher =
        new Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
            try {

              if (request.getPath().equalsIgnoreCase("/ws/lookup/inst/CSIROCXXX/coll/ANIC")) {
                return new MockResponse().setResponseCode(400);
              }

              if (request.getPath().equalsIgnoreCase("/ws/lookup/inst/CSIRO/coll/ANIC")) {
                File absolutePath = new File("src/test/resources/collectory/ANIC.json");
                String responseBody = FileUtils.readFileToString(absolutePath, "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
              }

              String datasetID =
                  request.getPath().substring(request.getPath().lastIndexOf("/") + 1);
              // read src
              File absolutePath = new File("src/test/resources/collectory/" + datasetID + ".json");

              if (!absolutePath.exists()) {
                return new MockResponse().setResponseCode(404);
              }

              String responseBody = FileUtils.readFileToString(absolutePath, "UTF-8");
              return new MockResponse()
                  .setResponseCode(200)
                  .setHeader("Content-Type", "application/json")
                  .setBody(responseBody);
            } catch (Exception e) {
              e.printStackTrace();
              throw new InterruptedException(e.getMessage());
            }
          }
        };
    server.setDispatcher(dispatcher);
    return server;
  }
}
