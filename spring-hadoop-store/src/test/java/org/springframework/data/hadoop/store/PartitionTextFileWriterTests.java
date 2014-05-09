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
package org.springframework.data.hadoop.store;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.output.PartitionTextFileWriter;
import org.springframework.data.hadoop.store.partition.AbstractPartitionKey;
import org.springframework.data.hadoop.store.partition.GenericPartitionKey;
import org.springframework.data.hadoop.store.partition.GenericPartitionStrategy;
import org.springframework.data.hadoop.store.partition.PartitionKey;
import org.springframework.data.hadoop.store.partition.PartitionKeyResolver;
import org.springframework.data.hadoop.store.partition.PartitionResolver;
import org.springframework.data.hadoop.store.partition.PartitionStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.SizeRolloverStrategy;

public class PartitionTextFileWriterTests extends AbstractStoreTests {

	@Test
	public void testWriteReadTextOneLine() throws IOException {
		String expression = "#headers[region] + '/' + dateFormat('yyyy/MM', #headers[timestamp])";
		String[] dataArray = new String[] { DATA10 };
		GenericPartitionStrategy<String> strategy = new GenericPartitionStrategy<String>(expression);

		GenericPartitionKey key1 = new GenericPartitionKey();
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("region", "foo");
		headers.put("timestamp", 0l);
		key1.setHeaders(headers);

		PartitionTextFileWriter<Map<String, Object>> writer =
				new PartitionTextFileWriter<Map<String,Object>>(testConfig, testDefaultPath, null, strategy);
		writer.setFileNamingStrategyFactory(new StaticFileNamingStrategy("bar"));

		writer.write(dataArray[0], key1);
		writer.flush();
		writer.close();

		// /tmp/TextFilePartitionedWriterTests/default/01/01/1970
		TextFileReader reader = new TextFileReader(testConfig, new Path(testDefaultPath, "foo/1970/01/bar"), null);
		TestUtils.readDataAndAssert(reader, dataArray);

	}

	@Test
	public void testWriteReadManyLinesWithNamingAndRollover() throws IOException {

		String expression = "#headers[region] + '/' + dateFormat('yyyy/MM', #headers[timestamp])";
		GenericPartitionStrategy<String> strategy = new GenericPartitionStrategy<String>(expression);

		PartitionTextFileWriter<Map<String, Object>> writer =
				new PartitionTextFileWriter<Map<String,Object>>(testConfig, testDefaultPath, null, strategy);

		writer.setFileNamingStrategyFactory(new RollingFileNamingStrategy());
		writer.setRolloverStrategyFactory(new SizeRolloverStrategy(40));

		long timestamp = 0;
		for (String data : DATA09ARRAY) {
			GenericPartitionKey key1 = new GenericPartitionKey();
			Map<String, Object> headers = new HashMap<String, Object>();
			headers.put("region", "foo");
			headers.put("timestamp", timestamp++);
			key1.put(GenericPartitionKey.KEY_HEADERS, headers);
			writer.write(data, key1);
		}
		writer.flush();
		writer.close();

		TextFileReader reader1 = new TextFileReader(testConfig, new Path(testDefaultPath, "foo/1970/01/0"), null);
		List<String> splitData1 = TestUtils.readData(reader1);

		TextFileReader reader2 = new TextFileReader(testConfig, new Path(testDefaultPath, "foo/1970/01/1"), null);
		List<String> splitData2 = TestUtils.readData(reader2);

		TextFileReader reader3 = new TextFileReader(testConfig, new Path(testDefaultPath, "foo/1970/01/2"), null);
		List<String> splitData3 = TestUtils.readData(reader3);

		assertThat(splitData1.size() + splitData2.size() + splitData3.size(), is(DATA09ARRAY.length));
	}

	@Test
	public void testCustomPartitioningKeys() throws IOException {
		String[] dataArray1 = new String[] { "customer1-1", "customer1-2", "customer1-3" };
		String[] dataArray2 = new String[] { "customer2-1", "customer2-2", "customer2-3" };
		String[] dataArray3 = new String[] { "customer3-1", "customer3-2", "customer3-3" };
		CustomerPartitionStrategy strategy = new CustomerPartitionStrategy();
		CustomerPartitionKey key1 = new CustomerPartitionKey("customer1");
		CustomerPartitionKey key2 = new CustomerPartitionKey("customer2");
		CustomerPartitionKey key3 = new CustomerPartitionKey("customer3");
		PartitionTextFileWriter<String> writer =
				new PartitionTextFileWriter<String>(testConfig, testDefaultPath, null, strategy);

		writer.write(dataArray1[0], key1);
		writer.write(dataArray1[1], key1);
		writer.write(dataArray1[2], key1);
		writer.write(dataArray2[0], key2);
		writer.write(dataArray2[1], key2);
		writer.write(dataArray2[2], key2);
		writer.write(dataArray3[0], key3);
		writer.write(dataArray3[1], key3);
		writer.write(dataArray3[2], key3);
		writer.flush();
		writer.close();

		// /tmp/TextFilePartitionedWriterTests/default/customer1
		TextFileReader reader1 = new TextFileReader(testConfig, new Path(testDefaultPath, "customer1"), null);
		TestUtils.readDataAndAssert(reader1, dataArray1);

		// /tmp/TextFilePartitionedWriterTests/default/customer2
		TextFileReader reader2 = new TextFileReader(testConfig, new Path(testDefaultPath, "customer2"), null);
		TestUtils.readDataAndAssert(reader2, dataArray2);

		// /tmp/TextFilePartitionedWriterTests/default/customer3
		TextFileReader reader3 = new TextFileReader(testConfig, new Path(testDefaultPath, "customer3"), null);
		TestUtils.readDataAndAssert(reader3, dataArray3);
	}

	@Test
	public void testCustomPartitionKeyResolving() throws IOException {
		String[] dataArray1 = new String[] { "customer1-1", "customer1-2", "customer1-3" };
		String[] dataArray2 = new String[] { "customer2-1", "customer2-2", "customer2-3" };
		String[] dataArray3 = new String[] { "customer3-1", "customer3-2", "customer3-3" };
		CustomerPartitionStrategy strategy = new CustomerPartitionStrategy();
		PartitionTextFileWriter<String> writer =
				new PartitionTextFileWriter<String>(testConfig, testDefaultPath, null, strategy);

		writer.write(dataArray1[0]);
		writer.write(dataArray1[1]);
		writer.write(dataArray1[2]);
		writer.write(dataArray2[0]);
		writer.write(dataArray2[1]);
		writer.write(dataArray2[2]);
		writer.write(dataArray3[0]);
		writer.write(dataArray3[1]);
		writer.write(dataArray3[2]);
		writer.flush();
		writer.close();

		// /tmp/TextFilePartitionedWriterTests/default/customer1
		TextFileReader reader1 = new TextFileReader(testConfig, new Path(testDefaultPath, "customer1"), null);
		TestUtils.readDataAndAssert(reader1, dataArray1);

		// /tmp/TextFilePartitionedWriterTests/default/customer2
		TextFileReader reader2 = new TextFileReader(testConfig, new Path(testDefaultPath, "customer2"), null);
		TestUtils.readDataAndAssert(reader2, dataArray2);

		// /tmp/TextFilePartitionedWriterTests/default/customer3
		TextFileReader reader3 = new TextFileReader(testConfig, new Path(testDefaultPath, "customer3"), null);
		TestUtils.readDataAndAssert(reader3, dataArray3);
	}

	private static class CustomerPartitionKey extends AbstractPartitionKey<String> {
		public CustomerPartitionKey(String partitionKey) {
			super(partitionKey);
		}
	}

	private static class CustomerPartitionStrategy implements PartitionStrategy<String, String> {

		CustomerPartitionResolver partitionResolver = new CustomerPartitionResolver();
		CustomerPartitionKeyResolver keyResolver = new CustomerPartitionKeyResolver();

		@Override
		public PartitionResolver<String> getPartitionResolver() {
			return partitionResolver;
		}

		@Override
		public PartitionKeyResolver<String, String> getPartitionKeyResolver() {
			return keyResolver;
		}
	}

	private static class CustomerPartitionResolver implements PartitionResolver<String> {

		@Override
		public Path resolvePath(PartitionKey<String> partitionKey) {
			return new Path(partitionKey.getValue());
		}
	}

	private static class CustomerPartitionKeyResolver implements PartitionKeyResolver<String, String> {

		@Override
		public PartitionKey<String> resolvePartitionKey(String entity) {
			if (entity.startsWith("customer1")) {
				return new CustomerPartitionKey("customer1");
			} else if (entity.startsWith("customer2")) {
				return new CustomerPartitionKey("customer2");
			} else if (entity.startsWith("customer3")) {
				return new CustomerPartitionKey("customer3");
			}
			return null;
		}
	}

}
