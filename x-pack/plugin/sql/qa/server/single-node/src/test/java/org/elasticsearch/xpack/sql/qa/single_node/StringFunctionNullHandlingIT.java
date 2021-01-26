/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * Tests the NULL handling of the SQL string functions. The functions should return with NULL if (in case of most functions)
 * any of their arguments are NULL, no matter if it is a NULL literal or a field with a NULL value.
 * 
 * The test class based on the example function calls (and argument specifications) generates all the 
 * permutations of the function arguments: specified value as literal, specified value as field, NULL as literal, NULL as field and 
 * tests the function calls with all the permutations with at least one NULL argument (field or literal) expecting NULL output.
 * 
 * To ignore any of the tests, add a .ignore() method call after the Fn ctors in the FUNCTION_CALLS list below, like:
 * <code>
 * new Fn("ASCII", "string").ignore()
 * </code>
 */
public class StringFunctionNullHandlingIT extends JdbcIntegrationTestCase {

    private static final List<Fn> FUNCTION_CALLS = asList(
        new Fn("ASCII", "string"),
        new Fn("BIT_LENGTH", "string"),
        new Fn("CHAR", 66),
        new Fn("CHAR_LENGTH", "string"),
        new Fn("CONCAT", "str1", "str2"),
        new Fn("INSERT", "string", 2, 3, "replacement"),
        new Fn("LCASE", "STRING"),
        new Fn("LEFT", "string", 3),
        new Fn("LENGTH", "string"),
        new Fn("LOCATE", "ri", "string", new ArgumentSpec(1, false)),
        new Fn("LTRIM", "   string"),
        new Fn("OCTET_LENGTH", "string"),
        new Fn("POSITION", "string", "ri"),
        new Fn("REPEAT", "string", 10),
        new Fn("REPLACE", "foo", "o", "bar"),
        new Fn("RIGHT", "string", 3),
        new Fn("RTRIM", "string   "),
        new Fn("SPACE", 5),
        new Fn("STARTS_WITH", "string", "str"),
        new Fn("SUBSTRING", "string", 1, 2),
        new Fn("TRIM", "  string   "),
        new Fn("UCASE", "string")
    );

    private enum Source { FIELD, LITERAL }
    
    private static class Fn {
        private final String name;
        private final List<ArgumentSpec> arguments;
        private boolean ignored = false;
        
        public Fn(String name, Object... arguments) {
            this.name = name;
            this.arguments = new ArrayList<>();
            for (Object a : arguments) {
                this.arguments.add(
                    ArgumentSpec.class.isAssignableFrom(a.getClass()) ? (ArgumentSpec) a : new ArgumentSpec(a, true, Source.values()));
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
    
    private static class ArgumentSpec {
        private final Object exampleValue;
        private boolean nullable;
        private final Source[] sources;

        public ArgumentSpec(Object exampleValue, boolean nullable, Source... sources) {
            this.exampleValue = exampleValue;
            this.nullable = nullable;
            this.sources = sources.length == 0 ? Source.values() : sources;
        }
        
        private List<Argument> options() {
            List<Argument> p = new ArrayList<>();
            for (Source s : sources) {
                p.add(new Argument(exampleValue, s));
                if (nullable) {
                    p.add(new Argument(null, s));
                }
            }
            return p;
        }
    }
    
    private static class Argument {
        private final Object value;
        private final Source source;

        private Argument(Object value, Source source) {
            this.value = value;
            this.source = source;
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> testFactory() {
        return FUNCTION_CALLS.stream().map(f -> new Object[]{f}).collect(Collectors.toList());
    }
    
    private final Fn fn;
    
    public StringFunctionNullHandlingIT(Fn fn) {
        this.fn = fn;
    }
    
    public void test() throws Exception {
        assumeFalse("Ignored", this.fn.ignored);
        
        // create a record for the function, where all the example (non-null) argument values are stored in fields
        // so the field mapping is automatically set up
        final String functionName = this.fn.name;
        String indexName = "test";
        String argPrefix = "arg_" + functionName + "_";
        {
            Map<String, Object> testDoc = new LinkedHashMap<>();
            testDoc.put("rowId", functionName);
            int idx = 0;
            for (ArgumentSpec argSpec : this.fn.arguments) {
                idx += 1;
                testDoc.put(argPrefix + idx, argSpec.exampleValue);
            }
            index(indexName, functionName, body -> body.mapContents(testDoc));
        }

        List<List<Argument>> optionsPerArgument = this.fn.arguments.stream().map(ArgumentSpec::options).collect(Collectors.toList());
        
        iterateAllPermutations(optionsPerArgument, arguments -> {
            // we only want to check the function calls that have at least a single NULL argument
            if (arguments.stream().noneMatch(o -> o.value == null)) {
                return;
            }
            
            List<String> functionCallArgs = new ArrayList<>();
            Map<String, Object> doc = new LinkedHashMap<>();
            int argIndex = 0;
            for (Argument arg : arguments) {
                argIndex += 1;
                String argInQuery = "not specified";
                switch (arg.source) {
                    case LITERAL:
                        if (arg.value == null) {
                            argInQuery = "NULL";
                        } else {
                            argInQuery = String.valueOf(arg.value);
                            if (arg.value instanceof String) {
                                argInQuery = "'" + argInQuery + "'";
                            }
                        }
                        break;
                    case FIELD:
                        final String argFieldName = argPrefix + argIndex;
                        doc.put(argFieldName, arg.value);
                        argInQuery = argFieldName;
                        break;
                }
                functionCallArgs.add(argInQuery);
            }
            
            final String testDocId = functionName + "_" + UUIDs.base64UUID();
            doc.put("rowId", testDocId);
            index(indexName, testDocId, body -> body.mapContents(doc));
            
            String functionCall = String.format(Locale.ROOT, "%s(%s)", functionName, String.join(", ", functionCallArgs));
            String query = String.format(Locale.ROOT, "SELECT %s FROM %s WHERE rowId = '%s'", functionCall, indexName, testDocId);
            ResultSet resultSet = esJdbc().createStatement().executeQuery(query);
            
            // only a single NULL value should be returned
            assertTrue(resultSet.next());
            assertNull("[" + query + "] \n for \n" + doc, resultSet.getObject(1));
            assertFalse(resultSet.next());
        });
    }
    
    public static <T> void iterateAllPermutations(List<List<T>> optionsPerItem, CheckedConsumer<List<T>, Exception> consumer) 
        throws Exception {
        
        if (optionsPerItem.isEmpty()) {
            consumer.accept(new ArrayList<>());
            return;
        }
        iterateAllPermutations(optionsPerItem.subList(1, optionsPerItem.size()), oneTailPermutation -> {
            for (T option : optionsPerItem.get(0)) {
                ArrayList<T> onePermutation = new ArrayList<>();
                onePermutation.add(option);
                onePermutation.addAll(oneTailPermutation);
                consumer.accept(onePermutation);
            }
        });
    }
    
}

