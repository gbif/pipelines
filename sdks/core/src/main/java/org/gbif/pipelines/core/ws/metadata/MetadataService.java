package org.gbif.pipelines.core.ws.metadata;

import java.util.List;
import okhttp3.ResponseBody;
import org.gbif.pipelines.core.ws.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.metadata.response.Installation;
import org.gbif.pipelines.core.ws.metadata.response.Network;
import org.gbif.pipelines.core.ws.metadata.response.Organization;
import org.gbif.pipelines.core.ws.metadata.response.PhylogenyFile;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;
import retrofit2.http.Streaming;

/** retro client for gbif services. */
public interface MetadataService {

  /**
   * get networks info of provided dataset uuid.
   *
   * @param datasetId datasetId
   * @return JsonElement with networks info for provided dataset uuid.
   */
  @GET("/v1/dataset/{datasetId}/networks")
  Call<List<Network>> getNetworks(@Path("datasetId") String datasetId);

  /**
   * get dataset info of provided dataset uuid.
   *
   * @param datasetId datasetId
   * @return JsonElement with provided dataset info.
   */
  @GET("/v1/dataset/{datasetId}")
  Call<Dataset> getDataset(@Path("datasetId") String datasetId);

  /**
   * get organization info of provided organization uuid.
   *
   * @param organizationId organizationId
   * @return JsonElement with organization info
   */
  @GET("/v1/organization/{organizationId}")
  Call<Organization> getOrganization(@Path("organizationId") String organizationId);

  /**
   * get installation info of provided installation uuid.
   *
   * @param installationKey installationKey
   * @return JsonElement with installation info
   */
  @GET("/v1/installation/{installationKey}")
  Call<Installation> getInstallation(@Path("installationKey") String installationKey);

  /**
   * get the phylogenetic tree files published with the provided dataset uuid.
   *
   * @param datasetKey datasetKey
   * @return list of phylogeny files of the dataset
   */
  @GET("/v1/experimental/dataset/{datasetKey}/phylogeny")
  Call<List<PhylogenyFile>> getPhylogenyFiles(@Path("datasetKey") String datasetKey);

  /**
   * get the content of a phylogenetic tree file published with the provided dataset uuid.
   *
   * @param datasetKey datasetKey
   * @param fileName name of the file as listed by {@link #getPhylogenyFiles(String)}
   * @return raw content of the tree file (Newick or Nexus)
   */
  @Streaming
  @GET("/v1/experimental/dataset/{datasetKey}/phylogeny/{fileName}")
  Call<ResponseBody> getPhylogenyFile(
      @Path("datasetKey") String datasetKey, @Path("fileName") String fileName);
}
