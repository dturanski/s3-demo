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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author David Turanski
 **/
@Configuration
public class AmazonS3Configuration {
	@Value("${aws.access.key}")
	private String awsAccessKey;

	@Value("${aws.secret.key}")
	private String awsSecretKey;

	@Bean
	public AmazonS3 amazonS3() {
		AWSCredentials credentials = new AWSCredentials() {
			@Override
			public String getAWSAccessKeyId() {
				return awsAccessKey;
			}

			@Override
			public String getAWSSecretKey() {
				return awsSecretKey;
			}
		};

		AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(credentials);

		return AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
	}

}
