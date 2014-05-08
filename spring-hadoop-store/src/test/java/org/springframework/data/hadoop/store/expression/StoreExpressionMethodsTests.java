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
package org.springframework.data.hadoop.store.expression;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class StoreExpressionMethodsTests {

	@Test
	public void testPartitionExpressions() {
		Map<String, Object> map = new HashMap<String, Object>();
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put("timestamp", 0l);
		headers.put("headerkey", "headervalue");
		map.put("headers", headers);
		map.put("timestamp", 0l);
		map.put("content", "mycontent");

		String nowYYYYMM = new SimpleDateFormat("yyyy/MM").format(new Date());

		ExpressionParser parser = new SpelExpressionParser();
		StandardEvaluationContext context = new StandardEvaluationContext(map);
		GenericPartitionKeyMethodResolver resolver = new GenericPartitionKeyMethodResolver();
		GenericPartitionKeyPropertyAccessor accessor = new GenericPartitionKeyPropertyAccessor();
		context.addMethodResolver(resolver);
		context.addPropertyAccessor(accessor);
		context.setVariables(map);

		assertThat(parser.parseExpression("dateFormat('yyyy/MM')").getValue(context, String.class), is("1970/01"));
		assertThat(parser.parseExpression("dateFormat('yyyy/MM', #headers[timestamp])").getValue(context, String.class), is("1970/01"));
		assertThat(parser.parseExpression("dateFormat('yyyy/MM', T(java.lang.System).currentTimeMillis())").getValue(context, String.class), is(nowYYYYMM));
		assertThat(parser.parseExpression("path(dateFormat('yyyy'),dateFormat('MM'))").getValue(context, String.class), is("1970/01"));
		assertThat(parser.parseExpression("dateFormat('yyyy') + '/' + dateFormat('MM')").getValue(context, String.class), is("1970/01"));
		assertThat(parser.parseExpression("headerkey").getValue(context, String.class), is("headervalue"));
		assertThat(parser.parseExpression("path(dateFormat('yyyy'),headerkey)").getValue(context, String.class), is("1970/headervalue"));
		assertThat(parser.parseExpression("#headers[timestamp]").getValue(context, String.class), is("0"));
	}

}
