/*
 * Copyright 2017 the original author or authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package io.spring.example.s3.demo.config;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Matchers.any;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { S3DemoStreamingTests.TestApplication.class}, properties = { "s3.dir=/testbucket/subdir" })
public class S3DemoStreamingTests {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
	public static final String BUCKET_NAME = "testbucket";

	private static List<S3Object> S3_OBJECTS;

	@BeforeClass
	public static void setup() throws IOException {
		File remoteFolder = TEMPORARY_FOLDER.newFolder("remote");

		File aFile = new File(remoteFolder, "a.test");
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<10; i++) {
			sb.append("hello" + i + "\n");
		}

		FileCopyUtils.copy(sb.toString().getBytes(), aFile);

		S3_OBJECTS = new ArrayList<>();

		for (File file : remoteFolder.listFiles()) {
			S3Object s3Object = new S3Object();
			s3Object.setBucketName(BUCKET_NAME);
			s3Object.setKey("subdir/" + file.getName());
			s3Object.setObjectContent(new FileInputStream(file));

			S3_OBJECTS.add(s3Object);
		}
	}

	@Autowired
	private MessageChannel output;

	@Autowired
	private MessageCollector messageCollector;

	@Test
	public void test() throws InterruptedException {
		for (int i = 0; i < 10; i++) {
			assertThat(messageCollector.forChannel(output).poll(1, TimeUnit.SECONDS).getPayload())
					.isEqualTo("hello" + i);
		}
	}

	@Configuration
	@EnableAutoConfiguration
	@Import(StreamingConfiguration.class)
	static class TestApplication {
		@Bean
		public AmazonS3 amazonS3() {
			AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);

			willAnswer(invocation -> {
				ObjectListing objectListing = new ObjectListing();
				List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
				for (S3Object s3Object : S3_OBJECTS) {
					S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
					s3ObjectSummary.setBucketName(BUCKET_NAME);
					s3ObjectSummary.setKey(s3Object.getKey());
					s3ObjectSummary.setLastModified(new Date(new File(s3Object.getKey()).lastModified()));
					objectSummaries.add(s3ObjectSummary);
				}
				return objectListing;
			}).given(amazonS3).listObjects(any(ListObjectsRequest.class));

			for (final S3Object s3Object : S3_OBJECTS) {
				willAnswer(invocation -> s3Object).given(amazonS3).getObject(BUCKET_NAME, s3Object.getKey());
			}

			return amazonS3;
		}
	}

}
