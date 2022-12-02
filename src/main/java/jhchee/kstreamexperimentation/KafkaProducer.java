package jhchee.kstreamexperimentation;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@AllArgsConstructor
@Component
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String word) {
        kafkaTemplate.send("input-topic", word)
                .thenAccept(result -> log.info("Message sent to topic: {}", word))
                .exceptionally(ex -> {
                    log.error("Failed to send message", ex);
                    return null;
                });
    }
}