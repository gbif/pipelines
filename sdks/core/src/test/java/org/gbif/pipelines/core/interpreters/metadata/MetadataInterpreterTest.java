package org.gbif.pipelines.core.interpreters.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.metadata.response.Installation;
import org.gbif.pipelines.core.ws.metadata.response.Network;
import org.gbif.pipelines.core.ws.metadata.response.Organization;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.junit.Test;

public class MetadataInterpreterTest {

  private static final String DATASET_ID = "7683cc47-cb13-4bad-9614-387c66aa8df0";
  private static final String INSTALLATION_KEY = "17a83780-3060-4851-9d6f-029d5fcb81c9";
  private static final String ORG_KEY = "d1fec91e-5b53-4cff-9b31-10fed530a3c3";

  @Test
  public void copiesDatasetCategoryFromRegistry() {
    MetadataServiceClient client =
        stubClient(Set.of("eDNA", "Observation"), Collections.emptyList());

    MetadataRecord result =
        MetadataInterpreter.interpret(
            client, DATASET_ID, 1, MetadataRecord.newBuilder().setId("1").build());

    assertEquals(
        Stream.of("eDNA", "Observation").sorted().toList(),
        result.getDatasetCategory().stream().sorted().toList());
  }

  @Test
  public void leavesDatasetCategoryEmptyWhenRegistryHasNoCategories() {
    MetadataServiceClient client = stubClient(null, Collections.emptyList());

    MetadataRecord result =
        MetadataInterpreter.interpret(
            client, DATASET_ID, 1, MetadataRecord.newBuilder().setId("1").build());

    assertTrue(result.getDatasetCategory().isEmpty());
  }

  private static MetadataServiceClient stubClient(Set<String> categories, List<Network> networks) {
    return new MetadataServiceClient() {
      @Override
      public Dataset getDataset(String datasetId) {
        Dataset dataset = new Dataset();
        dataset.setTitle("Test dataset");
        dataset.setLicense("http://creativecommons.org/licenses/by/4.0/legalcode");
        dataset.setInstallationKey(INSTALLATION_KEY);
        dataset.setPublishingOrganizationKey(ORG_KEY);
        if (categories != null) {
          dataset.setCategory(categories);
        }
        Endpoint endpoint = new Endpoint();
        endpoint.setType(EndpointType.DWC_ARCHIVE);
        dataset.setEndpoints(List.of(endpoint));
        return dataset;
      }

      @Override
      public List<Network> getNetworkFromDataset(String datasetId) {
        return networks;
      }

      @Override
      public Organization getOrganization(String organizationId) {
        Organization organization = new Organization();
        organization.setTitle("Publisher");
        organization.setCountry("BG");
        organization.setEndorsingNodeKey("node-key");
        return organization;
      }

      @Override
      public Installation getInstallation(String installationKey) {
        Installation installation = new Installation();
        installation.setOrganizationKey("hosting-org");
        return installation;
      }
    };
  }

  @Test
  public void copiesNetworkKeysWithoutAffectingDatasetCategory() {
    Network network = new Network();
    network.setKey("network-key");
    MetadataServiceClient client =
        stubClient(new LinkedHashSet<>(Set.of("eDNA")), List.of(network));

    MetadataRecord result =
        MetadataInterpreter.interpret(
            client, DATASET_ID, 1, MetadataRecord.newBuilder().setId("1").build());

    assertEquals(List.of("network-key"), result.getNetworkKeys());
    assertEquals(List.of("eDNA"), result.getDatasetCategory());
  }
}
