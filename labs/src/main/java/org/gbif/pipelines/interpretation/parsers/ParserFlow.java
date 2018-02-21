package org.gbif.pipelines.interpretation.parsers;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;

/**
 * Template class to create flowable parsers that decouple the error and result gathering while parsing a single value.
 * @param <IN> input type to be parsed
 * @param <OUT> expected output type
 */
public class ParserFlow<IN,OUT> {

  private Consumer<Throwable> errorHandler;
  private Consumer<OUT> resultHandler;
  private Consumer<IN> resultErrorHandler;

  private final Function<IN,OUT> parser;

  private final Map<Predicate<OUT>, BiConsumer<IN,OUT>> validations = new HashMap<>();

  //Used as the default consumer when a consumer is not specified for a filter
  private final BiConsumer<IN, OUT> voidConsumer = (i, o) -> {};

  /**
   * Creates an instance with the only required value: the function parser.
   * @param parser function to be applied to the input elements
   */
  private ParserFlow(Function<IN,OUT> parser) {
    this.parser = parser;
  }

  /**
   * Factory that builds a ParserFlow that applies the parser function.
   */
  public static <INU,OUTU> ParserFlow<INU,OUTU> of(Function<INU,OUTU> parser) {
    return new ParserFlow<>(parser);
  }

  /**
   * Applies a predicate, it the test failed the consumer is executed.
   */
  public ParserFlow<IN,OUT> withValidation(Predicate<OUT> filter, BiConsumer<IN,OUT> filterConsumer) {
    validations.put(filter, filterConsumer);
    return this;
  }

  /**
   * Adds a predicate to the list of validations.
   */
  public ParserFlow<IN,OUT> withValidation(Predicate<OUT> filter) {
    validations.put(filter, voidConsumer);
    return this;
  }

  public ParserFlow<IN,OUT> onException(Consumer<Throwable> errorHandler) {
    this.errorHandler = errorHandler;
    return this;
  }

  /**
   * Consumer of results.
   */
  public ParserFlow<IN,OUT> onSuccess(Consumer<OUT> resultHandler) {
    this.resultHandler = resultHandler;
    return this;
  }

  /**
   * Consumer to be executed when the parser can not interpret a value.
   */
  public ParserFlow<IN,OUT> onParseError(Consumer<IN> resultErrorHandler) {
    this.resultErrorHandler = resultErrorHandler;
    return this;
  }

  /**
   * Performs the parsing function.
   */
  public Optional<OUT> parse(IN input) {
    Preconditions.checkNotNull(resultHandler);
    try {
      Optional<OUT> result = Optional.ofNullable(parser.apply(input));
      if (Objects.nonNull(resultErrorHandler) && !result.isPresent()) {
        resultErrorHandler.accept(input);
      }
      if(result.isPresent()) {
        OUT parseResult = result.get();
        if(!matchValidation(input, parseResult)) {
          resultHandler.accept(parseResult);
          return Optional.empty();
        }
        return result;
      }
    } catch (Exception ex) {
      if (Objects.nonNull(errorHandler)) {
        errorHandler.accept(ex);
      }
    }
    return Optional.empty();
  }

  public <OUTP> Optional<OUTP> parseAndThen(IN input, ParserFlow<OUT, OUTP> parserTo) {
    return parse(input).flatMap(parserTo::parse);
  }

  public void parseAndThen(IN input, Consumer<OUT> consumer) {
    parse(input).ifPresent(consumer);
  }

  private boolean matchValidation(IN input, OUT result) {
    //TODO: there's gotta be a better way of doing this
    boolean anyMatch = false;
    for(Map.Entry<Predicate<OUT>, BiConsumer<IN,OUT>> filter : validations.entrySet()) {
      if (!filter.getKey().test(result)) {
        filter.getValue().accept(input,result);
        anyMatch = true;
      }
    }
    return anyMatch;
  }

}
