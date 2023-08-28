package au.org.ala.predicate;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.dwc.terms.*;
import org.gbif.predicate.query.SQLTermsMapper;

public class ALAEventTermsMapper implements SQLTermsMapper<ALAEventSearchParameter> {

  private static final Map<SearchParameter, ? extends Term> PARAM_TO_TERM =
      ImmutableMap.<SearchParameter, Term>builder()
          // DWC style formatting
          .put(ALAEventSearchParameter.countryCode, DwcTerm.countryCode)
          .put(ALAEventSearchParameter.datasetKey, GbifTerm.datasetKey)
          .put(ALAEventSearchParameter.eventHierarchy, ALASearchTerm.eventHierarchy)
          .put(ALAEventSearchParameter.eventTypeHierarchy, ALASearchTerm.eventTypeHierarchy)
          .put(ALAEventSearchParameter.locationID, DwcTerm.locationID)
          .put(ALAEventSearchParameter.measurementOfFactTypes, ALASearchTerm.measurementOfFactTypes)
          .put(ALAEventSearchParameter.month, DwcTerm.month)
          .put(ALAEventSearchParameter.samplingProtocol, DwcTerm.samplingProtocol)
          .put(ALAEventSearchParameter.stateProvince, DwcTerm.stateProvince)
          .put(ALAEventSearchParameter.taxonKey, GbifTerm.taxonKey)
          .put(ALAEventSearchParameter.year, DwcTerm.year)
          // GBIF API style formatting
          .put(ALAEventSearchParameter.COUNTRY_CODE, DwcTerm.countryCode)
          .put(ALAEventSearchParameter.DATASET_KEY, GbifTerm.datasetKey)
          .put(ALAEventSearchParameter.LOCATION_ID, DwcTerm.locationID)
          .put(ALAEventSearchParameter.MONTH, DwcTerm.month)
          .put(ALAEventSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .put(ALAEventSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
          .put(ALAEventSearchParameter.TAXON_KEY, GbifTerm.taxonKey)
          .put(ALAEventSearchParameter.YEAR, DwcTerm.year)
          .put(ALAEventSearchParameter.EVENT_TYPE, DwcTerm.eventType)
          .put(ALAEventSearchParameter.EVENT_TYPE_HIERARCHY, ALASearchTerm.eventTypeHierarchy)
          .build();
  private static final Map<SearchParameter, Term> ARRAY_STRING_TERMS =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(ALAEventSearchParameter.countryCode, DwcTerm.countryCode)
          .put(ALAEventSearchParameter.eventHierarchy, ALASearchTerm.eventHierarchy)
          .put(ALAEventSearchParameter.eventTypeHierarchy, ALASearchTerm.eventTypeHierarchy)
          .put(ALAEventSearchParameter.locationID, DwcTerm.locationID)
          .put(ALAEventSearchParameter.measurementOfFactTypes, ALASearchTerm.measurementOfFactTypes)
          .put(ALAEventSearchParameter.samplingProtocol, DwcTerm.samplingProtocol)
          .put(ALAEventSearchParameter.stateProvince, DwcTerm.stateProvince)
          .put(ALAEventSearchParameter.taxonKey, GbifTerm.taxonKey)
          .build();

  private static final Map<SearchParameter, Term> DENORMED_TERMS = Collections.emptyMap();

  @Override
  public Term term(ALAEventSearchParameter searchParameter) {
    return PARAM_TO_TERM.get(searchParameter);
  }

  @Override
  public boolean isArray(ALAEventSearchParameter searchParameter) {
    return ARRAY_STRING_TERMS.containsKey(searchParameter);
  }

  @Override
  public Term getTermArray(ALAEventSearchParameter searchParameter) {
    return ARRAY_STRING_TERMS.get(searchParameter);
  }

  @Override
  public boolean isDenormedTerm(ALAEventSearchParameter searchParameter) {
    return DENORMED_TERMS.containsKey(searchParameter);
  }

  @Override
  public ALAEventSearchParameter getDefaultGadmLevel() {
    return null;
  }
}
