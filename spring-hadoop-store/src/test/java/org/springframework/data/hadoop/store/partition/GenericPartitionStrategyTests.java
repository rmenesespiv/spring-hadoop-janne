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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Tests for {@link GenericPartitionStrategy}.
 *
 * @author Janne Valkealahti
 *
 */
public class GenericPartitionStrategyTests {

	@Test
	public void testSomething() {
		String expression = "#headers[region] + '/' + dateFormat('yyyy/MM', #headers[timestamp])";
		GenericPartitionStrategy<String> strategy = new GenericPartitionStrategy<String>(expression);
		GenericPartitionKey key = new GenericPartitionKey();
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("region", "foo");
		headers.put("timestamp", 0l);
		key.put(GenericPartitionKey.KEY_HEADERS, headers);

		Path resolvedPath = strategy.getPartitionResolver().resolvePath(key);
		assertThat(resolvedPath, notNullValue());

		PartitionKey<Map<String, Object>> resolvedPartitionKey = strategy.getPartitionKeyResolver().resolvePartitionKey("foo");
		assertThat(resolvedPartitionKey, nullValue());


	}

}
