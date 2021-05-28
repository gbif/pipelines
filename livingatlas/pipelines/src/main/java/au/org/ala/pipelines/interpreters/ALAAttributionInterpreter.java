package au.org.ala.pipelines.interpreters;

import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

import au.org.ala.kvs.client.ALACollectionLookup;
import au.org.ala.kvs.client.ALACollectionMatch;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.kvs.client.EntityReference;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.directory.api.util.Strings;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** Attribution interpreter providing additional attribution information to records. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAAttributionInterpreter {

  public static BiConsumer<ExtendedRecord, ALAAttributionRecord> interpretDatasetKey(
      String datasetId, KeyValueStore<String, ALACollectoryMetadata> dataResourceKvStore) {
    return (key, aar) -> {
      if (dataResourceKvStore != null && datasetId != null) {
        ALACollectoryMetadata m = dataResourceKvStore.get(datasetId);
        if (m != null) {
          aar.setDataResourceUid(m.getUid());
          if (m.getProvider() != null) {
            aar.setDataProviderUid(m.getProvider().getUid());
            aar.setDataProviderName(m.getProvider().getName());
          }
          aar.setDataResourceName(m.getName());
          aar.setLicenseType(m.getLicenseType());
          aar.setLicenseVersion(m.getLicenseVersion());
          aar.setProvenance(m.getProvenance());
          aar.setHasDefaultValues(
              m.getDefaultDarwinCoreValues() != null && !m.getDefaultDarwinCoreValues().isEmpty());

          // hub memberships
          List<EntityReference> hubs = m.getHubMembership();
          if (hubs != null && !hubs.isEmpty()) {
            List<org.gbif.pipelines.io.avro.EntityReference> refs = new ArrayList<>();
            aar.setHubMembership(refs);
            hubs.forEach(
                hub ->
                    refs.add(
                        org.gbif.pipelines.io.avro.EntityReference.newBuilder()
                            .setName(hub.getName())
                            .setUid(hub.getUid())
                            .setUri(hub.getUri())
                            .build()));
          }

        } else {
          throw new RuntimeException(
              "Unable to retrieve connection parameters for dataset: " + datasetId);
        }
      }
    };
  }

  public static BiConsumer<ExtendedRecord, ALAAttributionRecord> interpretCodes(
      KeyValueStore<ALACollectionLookup, ALACollectionMatch> collectionKvStore) {
    return (er, aar) -> {
      if (collectionKvStore != null) {

        String collectionCode =
            er.getCoreTerms()
                .get(DwcTerm.collectionCode.namespace() + DwcTerm.collectionCode.name());
        String institutionCode =
            er.getCoreTerms()
                .get(DwcTerm.institutionCode.namespace() + DwcTerm.institutionCode.name());

        if (!Strings.isEmpty(collectionCode)) {
          ALACollectionLookup lookup =
              ALACollectionLookup.builder()
                  .collectionCode(collectionCode)
                  .institutionCode(institutionCode)
                  .build();
          ALACollectionMatch m = collectionKvStore.get(lookup);

          // Only the collection code is in the lookup
          if (m.getCollectionUid() == null) {
            addIssue(aar, ALAOccurrenceIssue.UNRECOGNISED_COLLECTION_CODE.name());
          }
          aar.setCollectionUid(m.getCollectionUid());
          aar.setCollectionName(m.getCollectionName());
          aar.setInstitutionUid(m.getInstitutionUid());
          aar.setInstitutionName(m.getInstitutionName());
        }
      }
    };
  }
}
