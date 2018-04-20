package org.gbif.pipelines.core.parsers.memoize;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.core.Parsable;
import org.gbif.common.parsers.core.ParseResult;

import java.util.Objects;
import java.util.function.Function;


/**
 * Wrapper around the calculation of a parsing result.
 * @param <I> input value
 * @param <O> the result type of a parsing operation
 */
public class ParserMemoizer<I,O> {

    //Parser memoizer
    private final LoadingCache<I, O> memo;

    private static final int CACHE_MAX_SIZE = 10_000;

    private static final int INITIAL_CAPACITY = 1_000;

    //4 is the default value used by Guava internally
    private static final int CONCURRENCY_LEVEL = Math.max(Runtime.getRuntime().availableProcessors(), 4);

    private final O absentValue;

    /**
     * Default constructor to load values in the internal cache.
     * @param valueProvider function used to load values into the cache
     */
    private ParserMemoizer(Function<I,O> valueProvider, O absentValue) {
        memo = CacheBuilder.newBuilder().maximumSize(CACHE_MAX_SIZE)
                .concurrencyLevel(CONCURRENCY_LEVEL)
                .initialCapacity(INITIAL_CAPACITY)
                .build(CacheLoader.from(valueProvider::apply));
        this.absentValue = absentValue;
    }

    /**
     * Lookups an un parsed value, if the values is not in the cache it is added and returned.
     * @param unParsedValue raw un-parsed value
     * @return the parsed value of the input parameter
     */
    public O parse(I unParsedValue) {
      return Objects.nonNull(unParsedValue)? memo.getUnchecked(unParsedValue) : absentValue;
    }

    /**
     * Creates a memoizer wrapper around an instance of parsable class.
     * @param <P> type of the parse result
     * @return a memoizer wrapper
     */
    public static <P> ParserMemoizer<String,ParseResult<P>> memoize(Parsable<P> parsable) {
        return new ParserMemoizer<>(parsable::parse, null);
    }

    /**
     * Creates a memoizer wrapper around an instance of parsable class.
     * @param <P> type of the parse result
     * @return a memoizer wrapper
     */
    public static <I,O> ParserMemoizer<I,O> memoize(Function<I,O> parser) {
       return new ParserMemoizer<>(parser, null);
    }

    /**
     * Creates a memoizer wrapper around an instance of parsable class.
     * @param <P> type of the parse result
     * @param absentValue value when the key is null
     * @return a memoizer wrapper
     */
    public static <I,O> ParserMemoizer<I,O> memoize(Function<I,O> parser, O absentValue) {
        return new ParserMemoizer<>(parser, absentValue);
    }

    /**
     * Create a Memoizer wrapper to parse integer values.
     * @return a memoizer of a integer parser
     */
    public static ParserMemoizer<String,Integer> integerMemoizer() {
       return new ParserMemoizer<>(NumberParser::parseInteger, null);
    }

    /**
     * Create a Memoizer wrapper to parse double values.
     * @return a memoizer of a double parser
     */
    public static ParserMemoizer<String,Double> doubleMemoizer() {
       return new ParserMemoizer<>(NumberParser::parseDouble, null);
    }

}
