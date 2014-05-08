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

import org.springframework.data.hadoop.store.PartitionDataStoreWriter;

/**
 * A {@code PartitionKey} represents an instructions for {@link PartitionStrategy}
 * and {@link PartitionDataStoreWriter} how written entities should be partitioned.
 *
 * @author Janne Valkealahti
 *
 * @param <K> the type of partition key
 */
public interface PartitionKey<K> {

	/**
	 * Gets the value of this partition key.
	 *
	 * @return the partition key value
	 */
	K getValue();

}
