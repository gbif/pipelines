package au.org.ala.term;

import java.net.URI;
import org.gbif.dwc.terms.Term;

/** Set of terms in use by seedbank */
public enum SeedbankTerm implements Term {
  accessionNumber,
  adjustedGerminationPercentage,
  cultivated,
  darkHours,
  dateCollected,
  dateInStorage,
  dayTemperatureInCelsius,
  formInStorage,
  germinationRateInDays,
  lightHours,
  mediaSubstrate,
  nightTemperatureInCelsius,
  numberEmpty,
  numberFull,
  numberGerminated,
  numberPlantsSampled,
  numberTested,
  plantForm,
  pretreatment,
  primaryCollector,
  primaryStorageSeedBank,
  purityDebrisPercentage,
  purityPercentage,
  relativeHumidityPercentage,
  sampleSize,
  sampleWeightInGrams,
  seedPerGram,
  storageTemperatureInCelsius,
  testDateStarted,
  testLengthInDays,
  thousandSeedWeight,
  viabilityPercentage;
  private static final URI NS_URI = URI.create("http://ala.org.au/terms/seedbank/0.1/");

  SeedbankTerm() {}

  public String simpleName() {
    return this.name();
  }

  @Override
  public String prefixedName() {
    return Term.super.prefixedName();
  }

  @Override
  public String qualifiedName() {
    return Term.super.qualifiedName();
  }

  @Override
  public boolean isClass() {
    return false;
  }

  public String toString() {
    return this.prefixedName();
  }

  public String prefix() {
    return "seedbank";
  }

  public URI namespace() {
    return NS_URI;
  }
}
