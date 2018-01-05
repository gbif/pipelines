package org.gbif.pipelines.core.functions.ws.gbif.species;

import java.io.IOException;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

/**
 * DEMO IMPLEMENTATION ONLY! Much can be done to improve this... A function to set the nub fields of the interpreted
 * record.
 */
public class NubSpeciesMatchFunction implements SerializableFunction<TypedOccurrence, TypedOccurrence> {

  private static final Logger LOG = LoggerFactory.getLogger(NubSpeciesMatchFunction.class);

  @Override
  public TypedOccurrence apply(TypedOccurrence s) {

    Call<SpeciesMatchResponseModel> call = SpeciesMatchServiceRest.SINGLE
        .getService()
        .match(s); // i.e. not strict

    Response<SpeciesMatchResponseModel> r;
    try {
      r = call.execute();
    } catch (IOException e) {
      throw new RuntimeException("Unable to run species/match lookup", e);
    }

    if (!r.isSuccessful()) {
      LOG.warn("SpeciesMatchServiceRest response problem, response code - ", r.code());
      return s;
    }

    SpeciesMatchResponseModel model = r.body();
    if (model == null) {
      LOG.warn("SpeciesMatchServiceRest response problem, response body is null- ", r.code());
      return s;
    }

    return TypedOccurrenceMapper.mapFromSpeciesMatch(s, model);
  }

}
