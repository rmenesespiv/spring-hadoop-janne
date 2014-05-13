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
package org.springframework.data.hadoop.store.support;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;
import org.springframework.data.hadoop.store.TestUtils;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.strategy.naming.ChainedFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.UuidFileNamingStrategy;

/**
 * Tests for {@link OutputStoreObjectSupport}.
 *
 * @author Janne Valkealahti
 *
 */
public class OutputStoreObjectSupportTests {

	@Test
	public void testFindFiles() throws Exception {
		List<FileNamingStrategy> strategies = new ArrayList<FileNamingStrategy>();
		strategies.add(new StaticFileNamingStrategy("base"));
		strategies.add(new UuidFileNamingStrategy("fakeuuid", true));
		strategies.add(new RollingFileNamingStrategy());
		strategies.add(new StaticFileNamingStrategy("extension", "."));
		ChainedFileNamingStrategy strategy = new ChainedFileNamingStrategy(strategies);

		TestOutputStoreObjectSupport support = new TestOutputStoreObjectSupport(new Configuration(), new MockPath("/foo"), null);

		support.setInWritingSuffix(".tmp");
		support.setFileNamingStrategy(strategy);

		TestUtils.callMethod("initOutputContext", support);
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-1.extension"));
	}

	private static class TestOutputStoreObjectSupport extends OutputStoreObjectSupport {

		public TestOutputStoreObjectSupport(Configuration configuration, Path basePath, CodecInfo codec) {
			super(configuration, basePath, codec);
		}

	}

	static class MockFileSystem extends RawLocalFileSystem {
		@Override
		public FileStatus[] listStatus(Path pathPattern, PathFilter filter) throws IOException {
			ArrayList<FileStatus> files = new ArrayList<FileStatus>();

			if (filter.accept(new Path("/foo/basefakeuuid-0.extension.tmp"))) {
				files.add(new FileStatus(10, true, 1, 150, 150, pathPattern));
			}

			return files.toArray(new FileStatus[0]);
		}

		@Override
		public FileStatus[] listStatus(Path pathPattern) throws IOException {
			ArrayList<FileStatus> files = new ArrayList<FileStatus>();
			files.add(new FileStatus(10, true, 1, 150, 150, new Path("/foo/basefakeuuid-0.extension.tmp")));

			return files.toArray(new FileStatus[0]);
		}

		@Override
		public boolean exists(Path f) throws IOException {
			return true;
		}

	}

	static class MockPath extends Path {

		public MockPath(String pathString) throws IllegalArgumentException {
			super(pathString);
		}

		@Override
		public FileSystem getFileSystem(Configuration conf) throws IOException {
			return new MockFileSystem();
		}

	}
}
