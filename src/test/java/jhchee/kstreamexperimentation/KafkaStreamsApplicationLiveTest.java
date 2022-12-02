package jhchee.kstreamexperimentation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment;

@Slf4j
@Testcontainers
@SpringBootTest(classes = KstreamExperimentationApplication.class, webEnvironment = WebEnvironment.RANDOM_PORT)
class KafkaStreamsApplicationLiveTest {

    private final BlockingQueue<String> output = new LinkedBlockingQueue<>();

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    @TempDir
    private static File tempDir;

    @Autowired
    CountService countService;

    private KafkaMessageListenerContainer<Integer, String> consumer;


    @BeforeEach
    public void setUp() {
        output.clear();
        createConsumer();
    }

    @Test
    void givenInputMessages_whenGetCount_countsReceivedOnOutput() throws Exception {
        countService.addCount("test");
        countService.addCount("test");
        assertThat(output.poll(1, TimeUnit.MINUTES)).isEqualTo("test:1");
        assertThat(output.poll(1, TimeUnit.MINUTES)).isEqualTo("test:2");

        // reset after 5 seconds
        Thread.sleep(5_000);
        assertThat(output.poll(1, TimeUnit.MINUTES)).isEqualTo("test:0");
    }

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.streams.state.dir", tempDir::getAbsolutePath);
    }

    private void createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

        // set up the consumer for the word count output
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
        ContainerProperties containerProperties = new ContainerProperties("output-topic");
        consumer = new KafkaMessageListenerContainer<>(cf, containerProperties);
        consumer.setBeanName("test-consumer");
        consumer.setupMessageListener((MessageListener<String, Long>) record -> {
            log.info("Record received: {}", record);
            output.add(record.key() + ":" + record.value());
        });
        consumer.start();
    }

}