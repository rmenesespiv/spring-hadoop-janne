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

import java.text.SimpleDateFormat;
import java.util.Map;

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.TypedValue;

/**
 * A {@link MethodExecutor} handling formatting using a {@link SimpleDateFormat}.
 *
 * @author Janne Valkealahti
 *
 */
public class DateFormatMethodExecutor implements MethodExecutor {

	private String key;

	public DateFormatMethodExecutor(String key) {
		this.key = key;
	}

	@Override
	public TypedValue execute(EvaluationContext context, Object target, Object... arguments) throws AccessException {
		if (key == null) {
			SimpleDateFormat format = new SimpleDateFormat((String)arguments[0]);
			return new TypedValue(format.format((Long)arguments[1]));
		}
		if (target instanceof Map) {
			Map<?, ?> map = ((Map<?, ?>)target);
			SimpleDateFormat format = new SimpleDateFormat((String)arguments[0]);
			return new TypedValue(format.format(map.get(key)));
		}
		throw new AccessException("Unable to format");
	}

}
