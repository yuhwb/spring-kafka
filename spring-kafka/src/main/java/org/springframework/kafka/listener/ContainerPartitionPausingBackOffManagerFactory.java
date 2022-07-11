/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import org.springframework.util.Assert;

/**
 * A factory for {@link ContainerPartitionPausingBackOffManager}.
 *
 * @author Gary Russell
 * @since 2.9
 *
 */
public class ContainerPartitionPausingBackOffManagerFactory extends AbstractKafkaBackOffManagerFactory {

	private BackOffHandler backOffHandler;

	/**
	 * Construct an instance with the provided properties.
	 * @param listenerContainerRegistry the registry.
	 */
	public ContainerPartitionPausingBackOffManagerFactory(ListenerContainerRegistry listenerContainerRegistry) {
		super(listenerContainerRegistry);
	}

	@Override
	protected KafkaConsumerBackoffManager doCreateManager(ListenerContainerRegistry registry) {
		Assert.notNull(this.backOffHandler, "a BackOffHandler is required");
		return new ContainerPartitionPausingBackOffManager(getListenerContainerRegistry(), this.backOffHandler);
	}

	/**
	 * Set the back off handler to use in the created handlers.
	 * @param backOffHandler the handler.
	 */
	public void setBackOffHandler(BackOffHandler backOffHandler) {
		this.backOffHandler = backOffHandler;
	}

}