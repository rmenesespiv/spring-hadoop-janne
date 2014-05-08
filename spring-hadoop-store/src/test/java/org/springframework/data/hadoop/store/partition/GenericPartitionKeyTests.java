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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

public class GenericPartitionKeyTests {

	@Test
	public void testMessageHeaders() {
		Message<String> message = MessageBuilder.withPayload("foo").setHeader("myheader", "myvalue").build();
		GenericPartitionKey key = new GenericPartitionKey();
		key.addAllHeaders(message.getHeaders().entrySet());
		key.setContent(message.getPayload());
		key.setTimestamp(1);
		assertThat((String)key.getValue().get(GenericPartitionKey.KEY_CONTENT), is("foo"));
		assertThat((String)key.getHeader("myheader"), is("myvalue"));
		assertThat(key.getTimestamp(), greaterThan(0l));
	}

}
