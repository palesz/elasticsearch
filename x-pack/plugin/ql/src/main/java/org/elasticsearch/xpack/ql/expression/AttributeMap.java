/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

public class AttributeMap<E> {

    static class AttributeWrapper {

        private final Attribute attr;

        AttributeWrapper(Attribute attr) {
            this.attr = attr;
        }

        @Override
        public int hashCode() {
            return attr.semanticHash();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof AttributeWrapper) {
                AttributeWrapper aw = (AttributeWrapper) obj;
                return attr.semanticEquals(aw.attr);
            }

            return false;
        }

        @Override
        public String toString() {
            return attr.toString();
        }
    }

    /**
     * Set that does unwrapping of keys inside the keySet and iterator.
     */
    private abstract static class UnwrappingSet<W, U> extends AbstractSet<U> {
        private final Set<W> set;

        UnwrappingSet(Set<W> originalSet) {
            set = unmodifiableSet(originalSet);
        }

        @Override
        public Iterator<U> iterator() {
            return new Iterator<>() {
                final Iterator<W> i = set.iterator();

                @Override
                public boolean hasNext() {
                    return i.hasNext();
                }

                @Override
                public U next() {
                    return unwrap(i.next());
                }
            };
        }

        protected abstract U unwrap(W next);


        @Override
        public Stream<U> stream() {
            return set.stream().map(this::unwrap);
        }

        @Override
        public Stream<U> parallelStream() {
            return set.parallelStream().map(this::unwrap);
        }

        @Override
        public int size() {
            return set.size();
        }

        @Override
        public boolean equals(Object o) {
            return set.equals(o);
        }

        @Override
        public int hashCode() {
            return set.hashCode();
        }

        @Override
        public Object[] toArray() {
            Object[] array = set.toArray();
            for (int i = 0; i < array.length; i++) {
                array[i] = ((AttributeWrapper) array[i]).attr;
            }
            return array;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <A> A[] toArray(A[] a) {
            // collection is immutable so use that to our advantage
            if (a.length < size()) {
                a = (A[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size());
            }
            int i = 0;
            Object[] result = a;
            for (U u : this) {
                result[i++] = u;
            }
            // array larger than size, mark the ending element as null
            if (a.length > size()) {
                a[size()] = null;
            }
            return a;
        }

        @Override
        public String toString() {
            return set.toString();
        }
    }

    @SuppressWarnings("rawtypes")
    private static final AttributeMap EMPTY = new AttributeMap<>();

    @SuppressWarnings("unchecked")
    public static <E> AttributeMap<E> emptyAttributeMap() {
        return EMPTY;
    }

    private final Map<AttributeWrapper, E> delegate;
    private Set<Attribute> keySet = null;
    private Collection<E> values = null;

    public AttributeMap() {
        delegate = new LinkedHashMap<>();
    }

    public AttributeMap(Attribute key, E value) {
        delegate = singletonMap(new AttributeWrapper(key), value);
    }

    protected void add(Attribute key, E value) {
        delegate.put(new AttributeWrapper(key), value);
    }

    // a set from a collection of sets without (too much) copying
    protected void addAll(AttributeMap<E> other) {
        delegate.putAll(other.delegate);
    }
    
    AttributeMap<E> combine(AttributeMap<E> other) {
        AttributeMap<E> combine = new AttributeMap<>();
        combine.addAll(this);
        combine.addAll(other);

        return combine;
    }

    AttributeMap<E> subtract(AttributeMap<E> other) {
        AttributeMap<E> diff = new AttributeMap<>();
        for (Entry<AttributeWrapper, E> entry : this.delegate.entrySet()) {
            if (other.delegate.containsKey(entry.getKey()) == false) {
                diff.delegate.put(entry.getKey(), entry.getValue());
            }
        }

        return diff;
    }

    AttributeMap<E> intersect(AttributeMap<E> other) {
        AttributeMap<E> smaller = (other.size() > size() ? this : other);
        AttributeMap<E> larger = (smaller == this ? other : this);

        AttributeMap<E> intersect = new AttributeMap<>();
        for (Entry<AttributeWrapper, E> entry : smaller.delegate.entrySet()) {
            if (larger.delegate.containsKey(entry.getKey())) {
                intersect.delegate.put(entry.getKey(), entry.getValue());
            }
        }

        return intersect;
    }

    boolean subsetOf(AttributeMap<E> other) {
        if (this.size() > other.size()) {
            return false;
        }
        for (AttributeWrapper aw : delegate.keySet()) {
            if (other.delegate.containsKey(aw) == false) {
                return false;
            }
        }

        return true;
    }

    Set<String> attributeNames() {
        Set<String> s = new LinkedHashSet<>(size());

        for (AttributeWrapper aw : delegate.keySet()) {
            s.add(aw.attr.name());
        }
        return s;
    }

    public int size() {
        return delegate.size();
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }
    
    public E getOrDefault(Object key, E defaultValue) {
        if (key instanceof NamedExpression) {
            return delegate.getOrDefault(new AttributeWrapper(((NamedExpression) key).toAttribute()), defaultValue);
        }
        return defaultValue;
    }

    protected Set<Attribute> keySet() {
        if (keySet == null) {
            keySet = new UnwrappingSet<>(delegate.keySet()) {
                @Override
                protected Attribute unwrap(AttributeWrapper next) {
                    return next.attr;
                }
            };
        }
        return keySet;
    }
    
    protected Collection<E> values() {
        if (values == null) {
            values = unmodifiableCollection(delegate.values());
        }
        return values;
    }

    public void forEach(BiConsumer<? super Attribute, ? super E> action) {
        delegate.forEach((k, v) -> action.accept(k.attr, v));
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AttributeMap<?>) {
            obj = ((AttributeMap<?>) obj).delegate;
        }
        return delegate.equals(obj);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    public static <E> Builder<E> builder() {
        return new Builder<>();
    }

    public static <E> Builder<E> builder(AttributeMap<E> map) {
        return new Builder<E>().putAll(map);
    }

    public static class Builder<E> {
        private AttributeMap<E> map = new AttributeMap<>();

        private Builder() {}

        public Builder<E> put(Attribute attr, E value) {
            map.add(attr, value);
            return this;
        }

        public Builder<E> putAll(AttributeMap<E> m) {
            map.addAll(m);
            return this;
        }

        public AttributeMap<E> build() {
            return map;
        }
    }
}
