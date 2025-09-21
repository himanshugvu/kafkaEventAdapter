package com.orchestrator.core.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {
    
    private final OrchestratorProperties properties;
    
    public KafkaConfig(OrchestratorProperties properties) {
        this.properties = properties;
    }
    
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        
        String bootstrapServers = properties.producer().bootstrapServers() != null 
            ? properties.producer().bootstrapServers() 
            : properties.consumer().bootstrapServers();
            
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (properties.producer().acks() != null) {
            configProps.put(ProducerConfig.ACKS_CONFIG, properties.producer().acks());
        }
        configProps.put(ProducerConfig.RETRIES_CONFIG, properties.producer().retries());
        if (properties.producer().requestTimeout() != null) {
            configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) properties.producer().requestTimeout().toMillis());
        }
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, properties.producer().enableIdempotence());
        
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, properties.producer().batchSize());
        if (properties.producer().lingerMs() != null) {
            configProps.put(ProducerConfig.LINGER_MS_CONFIG, (int) properties.producer().lingerMs().toMillis());
        }
        if (properties.producer().compressionType() != null) {
            configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, properties.producer().compressionType());
        }
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, properties.producer().bufferMemory());
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, properties.producer().maxInFlightRequestsPerConnection());
        if (properties.producer().deliveryTimeout() != null) {
            configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) properties.producer().deliveryTimeout().toMillis());
        }
        
        if (properties.producer().enableIdempotence() && properties.producer().transactionIdPrefix() != null) {
            configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, properties.producer().transactionIdPrefix());
        }
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }
    
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    @Bean
    @ConditionalOnProperty(value = "orchestrator.producer.enable-idempotence", havingValue = "true")
    public KafkaTransactionManager kafkaTransactionManager() {
        return new KafkaTransactionManager(producerFactory());
    }
    
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.consumer().bootstrapServers());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, properties.consumer().groupId());
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.consumer().maxPollRecords());
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.consumer().enableAutoCommit());
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, properties.consumer().fetchMinBytes());
        if (properties.consumer().fetchMaxWait() != null) {
            configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, (int) properties.consumer().fetchMaxWait().toMillis());
        }
        configProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, properties.consumer().maxPartitionFetchBytes());
        configProps.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, properties.consumer().receiveBufferBytes());
        configProps.put(ConsumerConfig.SEND_BUFFER_CONFIG, properties.consumer().sendBufferBytes());
        if (properties.consumer().sessionTimeout() != null) {
            configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) properties.consumer().sessionTimeout().toMillis());
        }
        if (properties.consumer().heartbeatInterval() != null) {
            configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, (int) properties.consumer().heartbeatInterval().toMillis());
        }
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(properties.consumer().concurrency());
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        if (properties.consumer().pollTimeout() != null) {
            factory.getContainerProperties().setPollTimeout(properties.consumer().pollTimeout().toMillis());
        }
        
        return factory;
    }
}