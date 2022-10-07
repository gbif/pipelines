package au.org.ala.util;

import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.FileSystem;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.jetbrains.annotations.NotNull;

/**
 * Utilities for querying SOLR outputs and for creating configsets and collections in SOLR to
 * support integration tests.
 */
@Slf4j
public class SolrUtils {

  public static final String BIOCACHE_TEST_SOLR_COLLECTION = "biocache_test";
  public static final String BIOCACHE_CONFIG_SET = "biocache_test";

  public static List<String> getZkHosts() throws Exception {
    return Arrays.asList("localhost:" + System.getProperty("ZK_PORT"));
  }

  public static String getHttpHost() throws Exception {
    return "localhost:" + System.getProperty("SOLR_PORT");
  }

  public static void setupIndex() throws Exception {
    try {
      deleteSolrIndex();
    } catch (Exception e) {
      // expected for new setups
    }
    deleteSolrConfigSetIfExists(BIOCACHE_CONFIG_SET);
    createSolrConfigSet();
    createSolrIndex();
  }

  public static void createSolrConfigSet() throws Exception {
    // create a zip of
    try {
      FileUtils.forceDelete(new File("/tmp/configset.zip"));
    } catch (FileNotFoundException e) {
      // File isn't present on the first run of the test.
    }

    Map<String, String> env = new HashMap<>();
    env.put("create", "true");

    URI uri = URI.create("jar:file:/tmp/configset.zip");

    String absolutePath = new File(".").getAbsolutePath();
    String fullPath = absolutePath + "/solr/conf";

    if (!new File(fullPath).exists()) {
      // try in maven land
      fullPath = new File("../solr/conf").getAbsolutePath();
    }

    Path currentDir = Paths.get(fullPath);
    FileSystem zipfs = FileSystems.newFileSystem(uri, env);
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(currentDir)) {

      for (Path solrFilePath : stream) {
        if (!Files.isDirectory(solrFilePath)) {
          Path pathInZipfile = zipfs.getPath("/" + solrFilePath.getFileName());
          // Copy a file into the zip file
          Files.copy(solrFilePath, pathInZipfile, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
    zipfs.close();

    OkHttpClient client = new OkHttpClient();
    MediaType MEDIA_TYPE_OCTET = MediaType.parse("application/octet-stream");

    InputStream inputStream = new FileInputStream("/tmp/configset.zip");

    RequestBody requestBody = createRequestBody(MEDIA_TYPE_OCTET, inputStream);
    Request request =
        new Request.Builder()
            .url(
                "http://"
                    + getHttpHost()
                    + "/solr/admin/configs?action=UPLOAD&name="
                    + BIOCACHE_CONFIG_SET)
            .post(requestBody)
            .build();

    Response response = client.newCall(request).execute();
    if (!response.isSuccessful()) {
      throw new IOException("Unexpected code " + response);
    }

    log.info("POST {}", response.body().string());
  }

  public static void deleteSolrConfigSetIfExists(String configset) throws Exception {

    final SolrClient cloudSolrClient =
        new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    final ConfigSetAdminRequest.List adminRequest = new ConfigSetAdminRequest.List();

    ConfigSetAdminResponse.List adminResponse = adminRequest.process(cloudSolrClient);

    boolean exists = adminResponse.getConfigSets().contains(configset);

    if (exists) {
      final ConfigSetAdminRequest.Delete deleteRequest = new ConfigSetAdminRequest.Delete();
      deleteRequest.setConfigSetName(configset);
      deleteRequest.process(cloudSolrClient);
    }

    cloudSolrClient.close();
  }

  public static void createSolrIndex() throws Exception {

    final SolrClient cloudSolrClient =
        new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    final CollectionAdminRequest.Create adminRequest =
        CollectionAdminRequest.createCollection(
            BIOCACHE_TEST_SOLR_COLLECTION, BIOCACHE_CONFIG_SET, 1, 1);
    adminRequest.process(cloudSolrClient);
    cloudSolrClient.close();
  }

  public static void deleteSolrIndex() throws Exception {

    final SolrClient cloudSolrClient =
        new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();

    final CollectionAdminRequest.List listRequest = new CollectionAdminRequest.List();
    CollectionAdminResponse response = listRequest.process(cloudSolrClient);

    List<String> collections = (List<String>) response.getResponse().get("collections");

    if (collections != null && collections.contains(BIOCACHE_TEST_SOLR_COLLECTION)) {
      final CollectionAdminRequest.Delete adminRequest =
          CollectionAdminRequest.deleteCollection(BIOCACHE_TEST_SOLR_COLLECTION);
      adminRequest.process(cloudSolrClient);
    }

    cloudSolrClient.close();
  }

  public static void reloadSolrIndex() throws Exception {
    final SolrClient cloudSolrClient =
        new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    final CollectionAdminRequest.Reload adminRequest =
        CollectionAdminRequest.reloadCollection(BIOCACHE_TEST_SOLR_COLLECTION);
    adminRequest.process(cloudSolrClient);
    cloudSolrClient.close();
  }

  public static Optional<SolrDocument> getRecord(String queryUrl) throws Exception {
    CloudSolrClient solr = new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    solr.setDefaultCollection(BIOCACHE_TEST_SOLR_COLLECTION);

    SolrQuery params = new SolrQuery();
    params.setQuery(queryUrl);
    params.setSort("score", SolrQuery.ORDER.desc);
    params.setStart(0);
    params.setRows(100);

    QueryResponse response = solr.query(params);
    SolrDocumentList results = response.getResults();
    if (results.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(results.get(0));
    }
  }

  public static Long getRecordCount(String queryUrl) throws Exception {
    CloudSolrClient solr = new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    solr.setDefaultCollection(BIOCACHE_TEST_SOLR_COLLECTION);

    SolrQuery params = new SolrQuery();
    params.setQuery(queryUrl);
    params.setSort("score", SolrQuery.ORDER.desc);
    params.setStart(0);
    params.setRows(100);

    QueryResponse response = solr.query(params);
    SolrDocumentList results = response.getResults();
    return results.getNumFound();
  }

  public static SolrDocumentList getRecords(String queryUrl) throws Exception {
    CloudSolrClient solr = new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    solr.setDefaultCollection(BIOCACHE_TEST_SOLR_COLLECTION);

    SolrQuery params = new SolrQuery();
    params.setQuery(queryUrl);
    params.setSort("score", SolrQuery.ORDER.desc);
    params.setStart(0);
    params.setRows(100);

    QueryResponse response = solr.query(params);
    return response.getResults();
  }

  static RequestBody createRequestBody(final MediaType mediaType, final InputStream inputStream) {

    return new RequestBody() {
      @Override
      public MediaType contentType() {
        return mediaType;
      }

      @Override
      public long contentLength() {
        try {
          return inputStream.available();
        } catch (IOException e) {
          return 0;
        }
      }

      @Override
      public void writeTo(@NotNull BufferedSink sink) throws IOException {
        try (Source source = Okio.source(inputStream)) {
          sink.writeAll(source);
        }
      }
    };
  }
}
