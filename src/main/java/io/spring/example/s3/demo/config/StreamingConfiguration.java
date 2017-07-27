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
import io.spring.example.s3.demo.StreamReadingTextSplitter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.inbound.S3StreamingMessageSource;
import org.springframework.integration.aws.support.S3RemoteFileTemplate;
import org.springframework.integration.aws.support.S3SessionFactory;
import org.springframework.integration.aws.support.filters.S3PersistentAcceptOnceFileListFilter;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.file.remote.FileInfo;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.PollableChannel;
import org.springframework.scheduling.support.PeriodicTrigger;

import java.util.Comparator;

/**
 * Streaming Configuration for a Stream Source.
 *
 * @author David Turanski
 **/

@EnableBinding(Source.class)
public class StreamingConfiguration {
	@Value("${s3.dir}")
	private String S3_DIR;

	@Bean(name = PollerMetadata.DEFAULT_POLLER)
	public PollerMetadata defaultPoller() {

		PollerMetadata pollerMetadata = new PollerMetadata();
		pollerMetadata.setTrigger(new PeriodicTrigger(10));
		return pollerMetadata;
	}

	@Bean
	@InboundChannelAdapter(value = "s3Channel", poller = @Poller(fixedDelay = "100"))
	public S3StreamingMessageSource s3InboundStreamingMessageSource(AmazonS3 amazonS3) {

		S3RemoteFileTemplate s3FileTemplate = new S3RemoteFileTemplate(amazonS3);
		S3StreamingMessageSource s3MessageSource = new S3StreamingMessageSource(s3FileTemplate,
				Comparator.comparing(FileInfo::getFilename));
		s3MessageSource.setRemoteDirectory(S3_DIR);
		s3MessageSource.setFilter(new S3PersistentAcceptOnceFileListFilter(new SimpleMetadataStore(), "streaming"));
		return s3MessageSource;
	}

	@Bean
	@ServiceActivator(inputChannel = "s3Channel")
	public StreamReadingTextSplitter transformer(Source source) {
		return new StreamReadingTextSplitter(source.output());
	}

	@Bean
	public S3RemoteFileTemplate template(AmazonS3 amazonS3) {
		return new S3RemoteFileTemplate(new S3SessionFactory(amazonS3));
	}

	@Bean
	public PollableChannel s3Channel() {
		return new QueueChannel();
	}
}
