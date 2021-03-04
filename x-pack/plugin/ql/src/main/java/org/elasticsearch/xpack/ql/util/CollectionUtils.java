/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

public abstract class CollectionUtils {

    public static boolean isEmpty(Collection<?> col) {
        return col == null || col.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> combine(List<? extends T> left, List<? extends T> right) {
        if (right.isEmpty()) {
            return (List<T>) left;
        }
        if (left.isEmpty()) {
            return (List<T>) right;
        }

        List<T> list = new ArrayList<>(left.size() + right.size());
        if (left.isEmpty() == false) {
            list.addAll(left);
        }
        if (right.isEmpty() == false) {
            list.addAll(right);
        }
        return list;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> List<T> combine(Collection<? extends T>... collections) {
        if (org.elasticsearch.common.util.CollectionUtils.isEmpty(collections)) {
            return emptyList();
        }

        List<T> list = new ArrayList<>();
        for (Collection<? extends T> col : collections) {
            if (col instanceof Set) {
                for (T t : col) {
                    list.add(t);
                }
            }
            else {
                list.addAll(col);
            }
        }
        return list;
    }

    @SafeVarargs
    @SuppressWarnings({ "varargs", "unchecked" })
    public static <T> List<T> combineIterables(Iterable<? extends T>... iterables) {
        if (org.elasticsearch.common.util.CollectionUtils.isEmpty(iterables)) {
            return emptyList();
        }

        List<T> list = new ArrayList<>();
        for (Iterable<? extends T> col : iterables) {
            addAll(list, col);
        }
        return list;
    }
    
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> List<T> combine(Collection<? extends T> left, T... entries) {
        List<T> list = new ArrayList<>(left.size() + entries.length);
        if (left.isEmpty() == false) {
            list.addAll(left);
        }
        if (entries.length > 0) {
            Collections.addAll(list, entries);
        }
        return list;
    }
    
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> List<T> appendToIterable(Iterable<? extends T> left, T... entries) {
        List<T> list = null;
        if (left instanceof Collection) {
            list = new ArrayList<>(((Collection<? extends T>) left).size() + entries.length);
        } else {
            list = new ArrayList<>();
        }
        addAll(list, left);
        if (entries.length > 0) {
            Collections.addAll(list, entries);
        }
        return list;
    }

    public static int mapSize(int size) {
        if (size < 2) {
            return size + 1;
        }
        return (int) (size / 0.75f + 1f);
    }

    public static <T> void addAll(List<T> list, Iterable<? extends T> col) {
        // typically AttributeSet which ends up iterating anyway plus creating a redundant array
        if (col instanceof Set || col instanceof Collection == false) {
            for (T t : col) {
                list.add(t);
            }
        } else {
            if (((Collection<? extends T>) col).isEmpty() == false) {
                list.addAll((Collection<? extends T>) col);
            }
        }
    }
}
