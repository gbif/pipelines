package au.org.ala.pipelines.interpreters;

import au.org.ala.kvs.client.ALACollectionLookup;
import au.org.ala.kvs.client.ALACollectionMatch;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.*;

import java.util.function.BiConsumer;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAAttributionInterpreter {

  public static BiConsumer<ExtendedRecord, ALAAttributionRecord> interpretDatasetKey(
      MetadataRecord mr,
      KeyValueStore<String, ALACollectoryMetadata> dataResourceKvStore) {
    return (key, aar) -> {
      if (dataResourceKvStore != null) {
        if (mr.getId() != null) {
          ALACollectoryMetadata m = dataResourceKvStore.get(mr.getId());
          if (m != null) {
            aar.setDataResourceUid(m.getUid());
            if (m.getProvider() != null) {
              aar.setDataProviderUid(m.getProvider().getUid());
            }
            aar.setDataResourceName(m.getName());
            aar.setLicenseType(m.getLicenseType());
            aar.setLicenseVersion(m.getLicenseVersion());
          } else {
            throw new RuntimeException(
                "Unable to retrieve connection parameters for dataset: " + mr.getId());
          }
        }
      }
    };
  }

  public static BiConsumer<ExtendedRecord, ALAAttributionRecord> interpretCodes(
      KeyValueStore<ALACollectionLookup, ALACollectionMatch> collectionKvStore) {
    return (er, aar) -> {
      if (collectionKvStore != null) {

        String collectionCode = er.getCoreTerms()
            .get(DwcTerm.collectionCode.namespace() + DwcTerm.collectionCode.name());
        String institutionCode = er.getCoreTerms()
            .get(DwcTerm.collectionCode.namespace() + DwcTerm.collectionCode.name());

        if (collectionCode != null || institutionCode != null) {
          ALACollectionLookup lookup = ALACollectionLookup.builder()
              .collectionCode(collectionCode)
              .institutionCode(institutionCode)
              .build();
          ALACollectionMatch m = collectionKvStore.get(lookup);
          aar.setCollectionUid(m.getCollectionUid());
          aar.setCollectionName(m.getCollectionName());
          aar.setInstitutionUid(m.getInstitutionUid());
          aar.setInstitutionName(m.getInstitutionName());
        }
      }
    };
  }
}
