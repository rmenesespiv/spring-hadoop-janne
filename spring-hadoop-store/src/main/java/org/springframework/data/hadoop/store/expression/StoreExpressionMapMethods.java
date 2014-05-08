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

import java.util.Map;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * Helper class to work with a spel expressions resolving values
 * from a map.
 *
 * @author Janne Valkealahti
 *
 */
public class StoreExpressionMapMethods {

	private Map<String, Object> delegate;

	private EvaluationContext context;

	private boolean enableVariables;

	/**
	 * Instantiates a new store expression map methods.
	 *
	 * @param delegate the delegate
	 */
	public StoreExpressionMapMethods(Map<String, Object> delegate) {
		this(delegate, true);
	}

	/**
	 * Instantiates a new store expression map methods.
	 *
	 * @param delegate the delegate
	 * @param enableVariables the enable spel variables from a map
	 */
	public StoreExpressionMapMethods(Map<String, Object> delegate, boolean enableVariables) {
		this.enableVariables = enableVariables;
		setDelegate(delegate);
	}

	/**
	 * Gets the value.
	 *
	 * @param <T> the generic type
	 * @param expression the expression
	 * @param desiredResultType the desired result type
	 * @return the value
	 * @throws EvaluationException the evaluation exception
	 */
	public <T> T getValue(Expression expression, Class<T> desiredResultType) throws EvaluationException {
		Assert.notNull(expression, "Expression cannot be null");
		return expression.getValue(context, desiredResultType);
	}

	protected void setDelegate(Map<String, Object> delegate) {
		Assert.notNull(delegate, "Delegating map must be set");
		this.delegate = delegate;
		init();
	}

	private void init() {
		StandardEvaluationContext context = new StandardEvaluationContext(delegate);
		context.addMethodResolver(new GenericPartitionKeyMethodResolver());
		context.addPropertyAccessor(new GenericPartitionKeyPropertyAccessor());
		if (enableVariables) {
			context.setVariables(delegate);
		}
		this.context = context;
	}

}
