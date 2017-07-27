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

package io.spring.example.s3.demo;

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Consumes an {@link InputStream} of text and sends a {@link Message} for each line.
 *
 * @author David Turanski
 **/
public class StreamReadingTextSplitter {

	private final MessageChannel channel;

	/**
	 * @param channel the output channel used to send messages. Used as a ServiceActivator since the method returns
	 *                void.
	 */
	public StreamReadingTextSplitter(MessageChannel channel) {
		this.channel = channel;
	}

	public void splitStream(Message<?> message) throws Exception {
		Assert.isTrue(message.getPayload() instanceof InputStream, "payload must be an InputStream");
		InputStream stream = (InputStream) message.getPayload();
		BufferedReader in = new BufferedReader(new InputStreamReader(stream));
		String line;
		while ((line = in.readLine()) != null) {
			channel.send(MessageBuilder.createMessage(line, message.getHeaders()));
		}

		Closeable closeableResource = (new IntegrationMessageHeaderAccessor(message)).getCloseableResource();
		if (closeableResource != null) {
			closeableResource.close();
		}

	}
}
