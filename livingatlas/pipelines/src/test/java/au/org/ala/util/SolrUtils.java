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

  public static List<String> getZkHosts() throws Exception {
    return Arrays.asList("localhost:" + System.getProperty("ZK_PORT"));
  }

  public static String getHttpHost() throws Exception {
    return "localhost:" + System.getProperty("SOLR_PORT");
  }

  public static void setupIndex(String indexName) throws Exception {
    try {
      deleteSolrIndex(indexName);
    } catch (Exception e) {
      // expected for new setups
    }
    deleteSolrConfigSetIfExists(indexName);
    createSolrConfigSet(indexName);
    createSolrIndex(indexName);
  }

  public static void createSolrConfigSet(String indexName) throws Exception {
    // create a zip of
    try {
      FileUtils.forceDelete(new File("/tmp/configset_" + indexName + ".zip"));
    } catch (FileNotFoundException e) {
      // File isn't present on the first run of the test.
    }

    Map<String, String> env = new HashMap<>();
    env.put("create", "true");

    URI uri = URI.create("jar:file:/tmp/configset_" + indexName + ".zip");

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

    InputStream inputStream = new FileInputStream("/tmp/configset_" + indexName + ".zip");

    RequestBody requestBody = createRequestBody(MEDIA_TYPE_OCTET, inputStream);
    Request request =
        new Request.Builder()
            .url("http://" + getHttpHost() + "/solr/admin/configs?action=UPLOAD&name=" + indexName)
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

  public static void createSolrIndex(String indexName) throws Exception {

    final SolrClient cloudSolrClient =
        new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    final CollectionAdminRequest.Create adminRequest =
        CollectionAdminRequest.createCollection(indexName, indexName, 1, 1);
    adminRequest.process(cloudSolrClient);
    cloudSolrClient.close();
  }

  public static void deleteSolrIndex(String indexName) throws Exception {

    final SolrClient cloudSolrClient =
        new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();

    final CollectionAdminRequest.List listRequest = new CollectionAdminRequest.List();
    CollectionAdminResponse response = listRequest.process(cloudSolrClient);

    List<String> collections = (List<String>) response.getResponse().get("collections");

    if (collections != null && collections.contains(indexName)) {
      final CollectionAdminRequest.Delete adminRequest =
          CollectionAdminRequest.deleteCollection(indexName);
      adminRequest.process(cloudSolrClient);
    }

    cloudSolrClient.close();
  }

  public static void reloadSolrIndex(String indexName) throws Exception {
    final SolrClient cloudSolrClient =
        new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    final CollectionAdminRequest.Reload adminRequest =
        CollectionAdminRequest.reloadCollection(indexName);
    adminRequest.process(cloudSolrClient);
    cloudSolrClient.close();
  }

  public static Optional<SolrDocument> getRecord(String indexName, String queryUrl)
      throws Exception {
    CloudSolrClient solr = new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    solr.setDefaultCollection(indexName);

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

  public static Long getRecordCount(String indexName, String queryUrl) throws Exception {
    CloudSolrClient solr = new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    solr.setDefaultCollection(indexName);

    SolrQuery params = new SolrQuery();
    params.setQuery(queryUrl);
    params.setSort("score", SolrQuery.ORDER.desc);
    params.setStart(0);
    params.setRows(100);

    QueryResponse response = solr.query(params);
    SolrDocumentList results = response.getResults();
    return results.getNumFound();
  }

  public static SolrDocumentList getRecords(String indexName, String queryUrl) throws Exception {
    CloudSolrClient solr = new CloudSolrClient.Builder(getZkHosts(), Optional.empty()).build();
    solr.setDefaultCollection(indexName);

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
