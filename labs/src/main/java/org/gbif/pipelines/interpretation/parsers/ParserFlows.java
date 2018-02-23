package org.gbif.pipelines.interpretation.parsers;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.BasisOfRecordParser;
import org.gbif.common.parsers.BooleanParser;
import org.gbif.common.parsers.EstablishmentMeansParser;
import org.gbif.common.parsers.LifeStageParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.SexParser;
import org.gbif.common.parsers.TypeStatusParser;

public class ParserFlows {

  //Caching instance of BooleanParser since it is a file based parser
  private static final BooleanParser BOOLEAN_PARSER = BooleanParser.getInstance();

  private static final TypeStatusParser TYPE_PARSER = TypeStatusParser.getInstance();
  private static final BasisOfRecordParser BOR_PARSER = BasisOfRecordParser.getInstance();
  private static final SexParser SEX_PARSER = SexParser.getInstance();
  private static final EstablishmentMeansParser EST_PARSER = EstablishmentMeansParser.getInstance();
  private static final LifeStageParser LST_PARSER = LifeStageParser.getInstance();

  private ParserFlows() {
    //DO NOTHING
  }

  public static ParserFlow<String,Integer> intParserFlow() {
    return ParserFlow.of(NumberParser::parseInteger);
  }

  public static ParserFlow<String,Double> doubleParserFlow() {
    return ParserFlow.of(NumberParser::parseDouble);
  }

  public static ParserFlow<String,Boolean> booleanParserFlow() {
    return ParserFlow.of(val -> BOOLEAN_PARSER.parse(val).getPayload());
  }

  public static ParserFlow<String,TypeStatus> typeStatusParserFlow() {
    return ParserFlow.of(val -> TYPE_PARSER.parse(val).getPayload());
  }

  public static ParserFlow<String,BasisOfRecord> basisOfRecordParserFlow() {
    return ParserFlow.of(val -> BOR_PARSER.parse(val).getPayload());
  }

  public static ParserFlow<String,Sex> sexParserFlow() {
    return ParserFlow.of(val -> SEX_PARSER.parse(val).getPayload());
  }

  public static ParserFlow<String,EstablishmentMeans> establishmentMeansParserFlow() {
    return ParserFlow.of(val -> EST_PARSER.parse(val).getPayload());
  }

  public static ParserFlow<String,LifeStage> lifeStageParserFlow() {
    return ParserFlow.of(val -> LST_PARSER.parse(val).getPayload());
  }
}
