/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store.partition;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * {@link PartitionKey} which is keeping relevant information in
 * {@link Map} with key as {@link String} and value as {@link Object}.
 *
 * @author Janne Valkealahti
 *
 */
public class GenericPartitionKey extends AbstractPartitionKey<Map<String,Object>> {

	public final static String KEY_TIMESTAMP = "timestamp";

	public final static String KEY_HEADERS = "headers";

	public final static String KEY_CONTENT = "content";

	/**
	 * Instantiates a new generic partition key with a backing map as
	 * {@link HashMap}.
	 */
	public GenericPartitionKey() {
		this(new HashMap<String, Object>());
	}

	/**
	 * Instantiates a new generic partition key.
	 *
	 * @param <S> the generic type
	 * @param partitionKey the partition key
	 */
	public <S extends Map<String, Object>> GenericPartitionKey(S partitionKey) {
		super(partitionKey);
		if (!getValue().containsKey(KEY_HEADERS)) {
			put(KEY_HEADERS, new HashMap<String, Object>());
		}
	}

	/**
	 * Associates the specified value with the specified key in this
	 * partition key.
	 *
	 * @param key the key
	 * @param value the value
	 * @return the previous value associated with key, or null if there was no mapping for key.
	 */
	public Object put(String key, Object value) {
		return getValue().put(key, value);
	}

	/**
	 * Sets the timestamp.
	 *
	 * @param timestamp the new timestamp
	 */
	public void setTimestamp(long timestamp) {
		put(KEY_TIMESTAMP, timestamp);
	}

	public long getTimestamp() {
		return (Long) getValue().get(KEY_TIMESTAMP);
	}

	/**
	 * Sets the content.
	 *
	 * @param content the new content
	 */
	public void setContent(Object content) {
		put(KEY_CONTENT, content);
	}

	/**
	 * Adds the all headers.
	 *
	 * @param entries the entries
	 */
	public void addAllHeaders(Set<Entry<String, Object>> entries) {
		@SuppressWarnings("unchecked")
		Map<String, Object> headers = (Map<String, Object>) getValue().get(KEY_HEADERS);
		for (Entry<String, Object> entry : entries) {
			headers.put(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Sets the headers.
	 *
	 * @param headers the headers
	 */
	public void setHeaders(Map<String, Object> headers) {
		put(KEY_HEADERS, headers);
	}

	@SuppressWarnings("unchecked")
	public <T> T getHeader(String header) {
		Map<String, Object> headers = (Map<String, Object>) getValue().get(KEY_HEADERS);
		return (T) headers.get(header);
	}

}
