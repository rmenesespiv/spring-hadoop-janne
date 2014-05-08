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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.expression.StoreExpressionMapMethods;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * A {@link PartitionStrategy} which is used together with {@link GenericPartitionKey}
 * providing generic partitioning support using Spring SpEL.
 *
 * @author Janne Valkealahti
 *
 */
public class GenericPartitionStrategy<T extends Object> extends AbstractPartitionStrategy<T,Map<String, Object>> {

	private final static Log log = LogFactory.getLog(GenericPartitionStrategy.class);

	/**
	 * Instantiates a new generic partition strategy.
	 *
	 * @param expression the expression
	 */
	public GenericPartitionStrategy(Expression expression) {
		super(new GenericPartitionResolver(expression), new NoopPartitionKeyResolver<T>());
	}

	/**
	 * Instantiates a new generic partition strategy.
	 *
	 * @param expression the expression
	 */
	public GenericPartitionStrategy(String expression) {
		super(new GenericPartitionResolver(expression), new NoopPartitionKeyResolver<T>());
	}

	public static class GenericPartitionResolver implements PartitionResolver<Map<String, Object>> {

		private final Expression expression;

		public GenericPartitionResolver(Expression expression) {
			this.expression = expression;
			log.info("Using expression=[" + this.expression.getExpressionString() + "]");
		}

		public GenericPartitionResolver(String expression) {
			ExpressionParser parser = new SpelExpressionParser();
			this.expression = parser.parseExpression(expression);
			log.info("Using expression=[" + this.expression.getExpressionString() + "]");
		}

		@Override
		public Path resolvePath(PartitionKey<Map<String, Object>> partitionKey) {
			StoreExpressionMapMethods mapMethods = new StoreExpressionMapMethods(partitionKey.getValue());
			return new Path(mapMethods.getValue(expression, String.class));
		}

	}

	public static class NoopPartitionKeyResolver<T extends Object> implements PartitionKeyResolver<T,Map<String, Object>> {

		@Override
		public PartitionKey<Map<String, Object>> resolvePartitionKey(T entity) {
			return null;
		}

	}

}
