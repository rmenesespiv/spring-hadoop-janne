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

/**
 * Base implementation of a {@link PartitionKey}.
 *
 * @author Janne Valkealahti
 *
 * @param <K> the type of partition key
 */
public class AbstractPartitionKey<K> implements PartitionKey<K> {

	private final K partitionKey;

	/**
	 * Instantiates a new abstract partition key.
	 *
	 * @param <S> the generic type
	 * @param partitionKey the partition key
	 */
	public <S extends K> AbstractPartitionKey(S partitionKey) {
		this.partitionKey = partitionKey;
	}

	@Override
	public  K getValue() {
		return partitionKey;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((partitionKey == null) ? 0 : partitionKey.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AbstractPartitionKey<?> other = (AbstractPartitionKey<?>) obj;
		if (partitionKey == null) {
			if (other.partitionKey != null)
				return false;
		} else if (!partitionKey.equals(other.partitionKey))
			return false;
		return true;
	}

}
