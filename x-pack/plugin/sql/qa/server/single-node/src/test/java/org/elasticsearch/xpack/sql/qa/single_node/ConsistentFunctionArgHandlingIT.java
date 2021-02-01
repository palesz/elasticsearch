/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.collect.Tuple.tuple;

/**
 * This test was introduced because of the inconsistencies regarding NULL argument handling (NULL literal and NULL field value as argument
 * did not result in the same function return value).
 * 
 * In case of most of the functions, if any of their arguments are NULL the function should return with NULL value and 
 * in the functions should return with the same value no matter if the argument(s) came from a field or from a literal.
 * 
 * The test class based on the example function calls (and argument specifications) generates all the 
 * permutations of the function arguments (value/NULL as literal/field) and tests that the function calls with the same argument 
 * values provide the same result regardless of the source (literal, field) of the arguments, catching cases like:
 * 
 * <pre>
 * The result of the last call differs from the other calls:
 * 
 * string // INSERT('string'{F}, NULL{F}, 3{F}, 'replacement'{F})
 * string // INSERT('string'{L}, NULL{F}, 3{F}, 'replacement'{F})
 * null   // INSERT('string'{F}, NULL{L}, 3{F}, 'replacement'{F})
 * </pre>
 * 
 * Ignoring the tests:
 * 
 * To ignore any of the tests, add a .ignore() method call after the Fn ctors in the FUNCTION_CALLS list below, like:
 * <code>
 * new Fn("ASCII", "string").ignore()
 * </code>
 */
public class ConsistentFunctionArgHandlingIT extends JdbcIntegrationTestCase {

    private static final List<Fn> FUNCTION_CALLS = asList(
        new Fn("ASCII", "foobar"),
        new Fn("BIT_LENGTH", "foobar"),
        new Fn("CHAR", 66),
        new Fn("CHAR_LENGTH", "foobar"),
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

    private enum Source { FIELD, LITERAL }
    
    private static class Fn {
        private final String name;
        private final List<Argument> arguments;
        private boolean ignored = false;
        
        public Fn(String name, Object... arguments) {
            this.name = name;
            this.arguments = new ArrayList<>();
            for (Object a : arguments) {
                this.arguments.add(
                    Argument.class.isAssignableFrom(a.getClass()) ? (Argument) a : new Argument(a, Source.values()));
            }
        }
        
        public Fn ignore() {
            this.ignored = true;
            return this;
        }
        
        @Override
        public String toString() {
            return name + "(" + arguments.stream().map(a -> String.valueOf(a.exampleValue)).collect(Collectors.joining(", ")) + ")";
        }
    }
    
    private static class Argument {
        private final Object exampleValue;
        private final Source[] acceptedSources;

        public Argument(Object exampleValue, Source... acceptedSources) {
            this.exampleValue = exampleValue;
            this.acceptedSources = acceptedSources.length == 0 ? Source.values() : acceptedSources;
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> testFactory() {
        return FUNCTION_CALLS.stream().map(f -> new Object[]{f}).collect(toList());
    }
    
    private final Fn fn;
    
    public ConsistentFunctionArgHandlingIT(Fn fn) {
        this.fn = fn;
    }
    
    public void test() throws Exception {
        assumeFalse("Ignored", fn.ignored);

        // create a record for the function, where all the example (non-null) argument values are stored in fields
        // so the field mapping is automatically set up
        final String functionName = fn.name;
        String indexName = "test";
        String argPrefix = "arg_" + functionName + "_";
        {
            Map<String, Object> testDoc = new LinkedHashMap<>();
            testDoc.put("rowId", functionName);
            int idx = 0;
            for (Argument argSpec : fn.arguments) {
                idx += 1;
                testDoc.put(argPrefix + idx, argSpec.exampleValue);
            }
            index(indexName, functionName, body -> body.mapContents(testDoc));
        }
        
        iterateAllPermutations(fn.arguments.stream().map(a -> asList(a.exampleValue, null)).collect(toList()), argValues -> {
            // we only want to check the function calls that have at least a single NULL argument
            if (argValues.stream().noneMatch(Objects::isNull)) {
                return;
            }
            
            List<Tuple<String, Object>> results = new ArrayList<>();
            
            iterateAllPermutations(fn.arguments.stream().map(a -> asList(a.acceptedSources)).collect(toList()), argSources -> {
                List<String> functionCallArgs = new ArrayList<>();
                List<String> functionCallArgsForAssert = new ArrayList<>();
                Map<String, Object> doc = new LinkedHashMap<>();
                for (int argIndex = 0; argIndex < argValues.size(); argIndex++) {
                    Object argValue = argValues.get(argIndex);
                    Source argSource = argSources.get(argIndex);
                    String valueAsLiteral = asLiteralInQuery(argValue);
                    switch (argSource) {
                        case LITERAL:
                            functionCallArgs.add(valueAsLiteral);
                            break;
                        case FIELD:
                            final String argFieldName = argPrefix + (argIndex + 1);
                            doc.put(argFieldName, argValue);
                            functionCallArgs.add(argFieldName);
                            break;
                    }
                    functionCallArgsForAssert.add(valueAsLiteral + "{" + argSource.name().charAt(0) + "}");
                }
                final String testDocId = functionName + "_" + UUIDs.base64UUID();
                doc.put("rowId", testDocId);
                index(indexName, testDocId, body -> body.mapContents(doc));

                String functionCall = String.format(Locale.ROOT, "%s(%s)", functionName, String.join(", ", functionCallArgs));
                String query = String.format(Locale.ROOT, "SELECT %s FROM %s WHERE rowId = '%s'", functionCall, indexName, testDocId);
                ResultSet resultSet = esJdbc().createStatement().executeQuery(query);
                
                assertTrue(resultSet.next());
                results.add(tuple(
                    String.format(Locale.ROOT, "%s(%s)", functionName, String.join(", ", functionCallArgsForAssert)), 
                    resultSet.getObject(1)));
                // only a single row should be returned
                assertFalse(resultSet.next());
                
                if (results.stream().map(Tuple::v2).distinct().count() > 1) {
                    int maxResultWidth = results.stream().mapToInt(t -> String.valueOf(t.v2()).length()).max().orElse(20);
                    String resultsAsString = results.stream()
                            .map(r -> String.format("%2$-" + maxResultWidth + "s // %1$s", r.v1(), r.v2()))
                            .collect(Collectors.joining("\n"));
                    fail("The result of the last call differs from the other calls:\n" + resultsAsString);
                }
            });
        });
    }

    private String asLiteralInQuery(Object argValue) {
        String argInQuery;
        if (argValue == null) {
            argInQuery = "NULL";
        } else {
            argInQuery = String.valueOf(argValue);
            if (argValue instanceof String) {
                argInQuery = "'" + argInQuery + "'";
            }
        }
        return argInQuery;
    }

    public static <T> void iterateAllPermutations(List<List<T>> possibleValuesPerItem, CheckedConsumer<List<T>, Exception> consumer) 
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

