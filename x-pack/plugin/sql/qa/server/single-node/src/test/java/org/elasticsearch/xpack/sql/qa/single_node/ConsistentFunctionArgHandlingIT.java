/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.lang.String.join;
import static java.time.OffsetDateTime.now;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.collect.Tuple.tuple;
import static org.hamcrest.collection.IsEmptyCollection.empty;

/**
 * <p>This test was introduced because of the inconsistencies regarding NULL argument handling. NULL literal vs NULL field
 * value as function arguments in some case result in different function return values.</p>
 *
 * <p>Functions should return with the same value no matter if the argument(s) came from a field or from a literal.</p>
 *
 * <p>The test class based on the example function calls (and argument specifications) generates all the
 * permutations of the function arguments (4 options per argument: value/NULL as literal/field) and tests that the
 * function calls with the same argument values provide the same result regardless of the source (literal, field)
 * of the arguments.</p>
 *
 * <p>To ignore any of the tests, add an .ignore() method call after the Fn ctors in the FUNCTION_CALLS_TO_TEST list below, like:
 * <code> new Fn("ASCII", "foobar").ignore()</code></p>
 */
public class ConsistentFunctionArgHandlingIT extends JdbcIntegrationTestCase {

    private static final List<Fn> FUNCTION_CALLS_TO_TEST = asList(

        // Date functions
        new Fn("CURRENT_DATE").aliases("CURDATE", "TODAY"),
        new Fn("CURRENT_TIME", /* precision */ 5).aliases("CURTIME"),
        new Fn("CURRENT_TIMESTAMP", /* precision */ 4).aliases("NOW"),
        new Fn("DAY_NAME", now()).aliases("DAYNAME"),
        new Fn("DAY_OF_MONTH", now()).aliases("DAYOFMONTH", "DAY", "DOM"),
        new Fn("DAY_OF_WEEK", now()).aliases("DAYOFWEEK", "DOW"),
        new Fn("DAY_OF_YEAR", now()).aliases("DAYOFYEAR", "DOY"),
        new Fn("DATEADD", "weekday", 2, now()).aliases("DATE_ADD", "TIMESTAMPADD", "TIMESTAMP_ADD"),
        new Fn("DATEDIFF", "day", now().minusDays(500), now()).aliases("DATE_DIFF", "TIMESTAMPDIFF", "TIMESTAMP_DIFF"),
        new Fn("DATE_PARSE", "2020", "yyyy"),
        new Fn("DATEPART", "weekday", now()).aliases("DATE_PART"),
        new Fn("DATETIME_FORMAT", now(), "yyyy"),
        new Fn("DATETIME_PARSE", "07/04/2020 10:20:30.123", "dd/MM/uuuu HH:mm:ss.SSS"),
        new Fn("DATETRUNC", "month", now()).aliases("DATE_TRUNC"),
        new Fn("FORMAT", now(), "dd/MM/YYYY HH:mm:ss.ff"),
        new Fn("TO_CHAR", now(), "DD/MM/YYYY HH24:MI:SS.FF2"),
        new Fn("HOUR_OF_DAY", now()).aliases("HOUR"),
        new Fn("ISO_DAY_OF_WEEK", now()).aliases("ISODAYOFWEEK", "ISODOW", "IDOW"),
        new Fn("ISO_WEEK_OF_YEAR", now()).aliases("ISOWEEKOFYEAR", "ISOWEEK", "IWOY", "IW"),
        new Fn("MINUTE_OF_DAY", now()),
        new Fn("MINUTE_OF_HOUR", now()).aliases("MINUTE"),
        new Fn("MONTH_NAME", now()).aliases("MONTHNAME"),
        new Fn("MONTH_OF_YEAR", now()).aliases("MONTH"),
        new Fn("SECOND_OF_MINUTE", now()).aliases("SECOND"),
        new Fn("TIME_PARSE", "10:20:30.123", "HH:mm:ss.SSS"),
        new Fn("QUARTER", now()),
        new Fn("YEAR", now()),
        new Fn("WEEK_OF_YEAR", now()).aliases("WEEK"),
        // Math functions
        new Fn("ABS", -10.23),
        new Fn("ACOS", 123.12),
        new Fn("ASIN", 123.12),
        new Fn("ATAN", 123.12),
        new Fn("ATAN2", 123.12, 12.34),
        new Fn("CBRT", 123.12),
        new Fn("CEIL", -134.56).aliases("CEILING"),
        new Fn("COS", 123.12),
        new Fn("COSH", 123.12),
        new Fn("COT", 123.12),
        new Fn("DEGREES", 123.12),
        new Fn("E"),
        new Fn("EXP", 5.67),
        new Fn("EXPM1", 5.67),
        new Fn("FLOOR", -134.43),
        new Fn("LOG", 5),
        new Fn("LOG10", 5),
        new Fn("MOD", 1234, 56),
        new Fn("PI"),
        new Fn("POWER", 2.34, 5.6),
        new Fn("RADIANS", 123.45),
        new Fn("RANDOM", 1234.56).aliases("RAND"),
        new Fn("ROUND", -1234.56),
        new Fn("SIGN", -1234.56).aliases("SIGNUM"),
        new Fn("SIN", 123.45),
        new Fn("SINH", 123.45),
        new Fn("SQRT", 123.45),
        new Fn("TAN", 123.45),
        new Fn("TRUNCATE", 1234.567, 2).aliases("TRUNC"),
        // String functions
        new Fn("ASCII", "foobar"),
        new Fn("BIT_LENGTH", "foobar"),
        new Fn("CHAR", 66),
        new Fn("CHAR_LENGTH", "foobar").aliases("CHARACTER_LENGTH"),
        new Fn("CONCAT", "foo", "bar"),
        new Fn("INSERT", "foobar", 2, 3, "replacement"),
        new Fn("LCASE", "STRING"),
        new Fn("LEFT", "foobar", 3),
        new Fn("LENGTH", "foobar"),
        new Fn("LOCATE", "ob", "foobar", 1),
        new Fn("LTRIM", "   foobar"),
        new Fn("OCTET_LENGTH", "foobar"),
        new Fn("POSITION", "foobar", "ob"),
        new Fn("REPEAT", "foobar", 10),
        new Fn("REPLACE", "foo", "o", "bar"),
        new Fn("RIGHT", "foobar", 3),
        new Fn("RTRIM", "foobar   "),
        new Fn("SPACE", 5),
        new Fn("STARTS_WITH", "foobar", "foo"),
        new Fn("SUBSTRING", "foobar", 1, 2),
        new Fn("TRIM", "  foobar   "),
        new Fn("UCASE", "foobar")
    );

    private static final List<String> NON_TESTED_FUNCTIONS;
    static {
        try {
            Class<?> c = ConsistentFunctionArgHandlingIT.class;
            NON_TESTED_FUNCTIONS = Files.readAllLines(Path.of(c.getResource(c.getSimpleName() + "-non-tested-functions.txt").toURI()));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private enum Source {
        FIELD,
        LITERAL
    }

    private static class Fn {
        private final String name;
        private final List<Argument> arguments;
        private List<String> aliases = new ArrayList<>();
        private boolean ignored = false;

        private Fn(String name, Object... arguments) {
            this.name = name;
            this.arguments = new ArrayList<>();
            for (Object a : arguments) {
                this.arguments.add(new Argument(a));
            }
        }

        public Fn aliases(String... aliases) {
            this.aliases = asList(aliases);
            return this;
        }

        public Fn ignore() {
            this.ignored = true;
            return this;
        }

        @Override
        public String toString() {
            return name + "(" + arguments.stream().map(a -> String.valueOf(a.exampleValue)).collect(joining(", ")) + ")";
        }
    }

    private static class Argument {
        private final Object exampleValue;
        private final Source[] acceptedSources;

        private Argument(Object exampleValue, Source... acceptedSources) {
            this.exampleValue = exampleValue;
            this.acceptedSources = acceptedSources.length == 0 ? Source.values() : acceptedSources;
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> testFactory() {
        List<Object[]> tests = new ArrayList<>();
        final String functionToDebug = null;
        if (functionToDebug != null) {
            FUNCTION_CALLS_TO_TEST.stream().filter(f -> f.name.equals(functionToDebug)).forEach(f -> tests.add(new Object[] { f }));
        } else {
            tests.add(new Object[] { null });
            FUNCTION_CALLS_TO_TEST.forEach(f -> tests.add(new Object[] { f }));
        }
        return tests;
    }

    private final Fn fn;

    public ConsistentFunctionArgHandlingIT(Fn fn) {
        this.fn = fn;
    }

    public void test() throws Exception {
        if (fn == null) {
            checkScalarFunctionCoverage();
            return;
        }

        assumeFalse("Ignored", fn.ignored);
        assumeFalse("Function without arguments, nothing to test", fn.arguments.isEmpty());

        // create a record for the function, where all the example (non-null) argument values are stored in fields
        // so the field mapping is automatically set up
        final String functionName = fn.name;
        final String indexName = "test";
        final String argPrefix = "arg_" + functionName + "_";
        final String nullArgPrefix = "arg_null_" + functionName + "_";
        final String testDocId = functionName + "_" + UUIDs.base64UUID();

        indexTestDocForFunction(functionName, indexName, argPrefix, nullArgPrefix, testDocId);

        List<List<Object>> possibleValuesPerArguments = fn.arguments.stream().map(a -> asList(a.exampleValue, null)).collect(toList());
        List<List<Source>> acceptedSourcesPerArguments = fn.arguments.stream().map(a -> asList(a.acceptedSources)).collect(toList());

        // will initialize once to use the same timezone across all the requests
        final Connection jdbcConnection = esJdbc();

        iterateAllPermutations(possibleValuesPerArguments, argValues -> {
            // we only want to check the function calls that have at least a single NULL argument
            /*if (argValues.stream().noneMatch(Objects::isNull)) {
                return;
            }*/

            List<Tuple<String, Object>> results = new ArrayList<>();

            iterateAllPermutations(acceptedSourcesPerArguments, argSources -> {
                List<String> functionCallArgs = new ArrayList<>();
                List<String> functionCallArgsForAssert = new ArrayList<>();
                for (int argIndex = 0; argIndex < argValues.size(); argIndex++) {
                    final Object argValue = argValues.get(argIndex);
                    final Source argSource = argSources.get(argIndex);
                    final String valueAsLiteral = asLiteralInQuery(argValue);
                    switch (argSource) {
                        case LITERAL:
                            functionCallArgs.add(valueAsLiteral);
                            break;
                        case FIELD:
                            final String argFieldName = (argValue == null ? nullArgPrefix : argPrefix) + (argIndex + 1);
                            functionCallArgs.add(argFieldName);
                            break;
                    }
                    functionCallArgsForAssert.add(valueAsLiteral + "{" + argSource.name().charAt(0) + "}");
                }

                final String functionCall = functionName + "(" + join(", ", functionCallArgs) + ")";
                final String query = "SELECT " + functionCall + ", " + join(", ", functionCallArgs) + " FROM " + indexName + " WHERE docId = '" + testDocId + "'";
                logger.info(query);
                ResultSet retVal = jdbcConnection.createStatement().executeQuery(query);

                assertTrue(retVal.next());
                results.add(tuple(functionName + "(" + join(", ", functionCallArgsForAssert) + ")", retVal.getObject(1)));
                for (int i = 1; i <= retVal.getMetaData().getColumnCount(); i++) {
                    logger.info(retVal.getMetaData().getColumnName(i) + ": " + resultValueAsString(retVal.getObject(i)));
                }
                // only a single row should be returned
                assertFalse(retVal.next());

                if (results.stream().map(Tuple::v2).distinct().count() > 1) {
                    int maxResultWidth = results.stream().map(Tuple::v2).mapToInt(o -> resultValueAsString(o).length()).max().orElse(20);
                    String resultsAsString = results.stream()
                        .map(r -> String.format(Locale.ROOT, "%2$-" + maxResultWidth + "s // %1$s", r.v1(), resultValueAsString(r.v2())))
                        .collect(joining("\n"));
                    fail("The result of the last call differs from the other calls:\n" + resultsAsString);
                }
            });
        });
    }

    private void indexTestDocForFunction(String functionName, String indexName, String argPrefix, String nullArgPrefix, String testDocId)
        throws IOException {
        Map<String, Object> testDoc = new LinkedHashMap<>();
        testDoc.put("docId", testDocId);
        int idx = 0;
        for (Argument arg : fn.arguments) {
            idx += 1;
            Object valueToIndex = arg.exampleValue;
            if (valueToIndex instanceof OffsetDateTime) {
                valueToIndex = String.valueOf(valueToIndex);
            }
            testDoc.put(argPrefix + idx, valueToIndex);
            // first set the same value, so the mapping is populated for the null columns
            testDoc.put(nullArgPrefix + idx, valueToIndex);
        }
        index(indexName, functionName, body -> body.mapContents(testDoc));

        // zero out the fields to be used as nulls
        for (idx = 1; idx <= fn.arguments.size(); idx++) {
            testDoc.put(nullArgPrefix + idx, null);
        }
        index(indexName, functionName, body -> body.mapContents(testDoc));
    }

    private void checkScalarFunctionCoverage() throws Exception {
        ResultSet resultSet = esJdbc().createStatement().executeQuery("SHOW FUNCTIONS");
        Set<String> functions = new LinkedHashSet<>();
        while (resultSet.next()) {
            String name = resultSet.getString(1);
            String fnType = resultSet.getString(2);
            if ("SCALAR".equals(fnType)) {
                functions.add(name);
            }
        }
        for (Fn fn : FUNCTION_CALLS_TO_TEST) {
            functions.remove(fn.name);
            functions.removeAll(fn.aliases);
        }
        functions.removeAll(NON_TESTED_FUNCTIONS);

        assertThat("Some functions are not covered by this test", functions, empty());
    }
    
    private static String resultValueAsString(Object value) {
        return asLiteralInQuery(value) + " [" + (value == null ? null : value.getClass().getSimpleName()) + "]"; 
    }

    private static String asLiteralInQuery(Object argValue) {
        String argInQuery;
        if (argValue == null) {
            argInQuery = "NULL";
        } else {
            
            if (argValue instanceof String) {
                argInQuery = "'" + argValue + "'";
            } else if (argValue instanceof OffsetDateTime) {
                argInQuery = "'" + ((OffsetDateTime) argValue) + "'::datetime";
            } else {
                argInQuery = String.valueOf(argValue);
            }
        }
        return argInQuery;
    }

    private static <T> void iterateAllPermutations(List<List<T>> possibleValuesPerItem, CheckedConsumer<List<T>, Exception> consumer)
        throws Exception {

        if (possibleValuesPerItem.isEmpty()) {
            consumer.accept(new ArrayList<>());
            return;
        }
        iterateAllPermutations(possibleValuesPerItem.subList(1, possibleValuesPerItem.size()), onePermutationOfTail -> {
            for (T option : possibleValuesPerItem.get(0)) {
                ArrayList<T> onePermutation = new ArrayList<>();
                onePermutation.add(option);
                onePermutation.addAll(onePermutationOfTail);
                consumer.accept(onePermutation);
            }
        });
    }

}
