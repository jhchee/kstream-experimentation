package jhchee.kstreamexperimentation;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class CountService {
    private final KafkaProducer kafkaProducer;

    public void addCount(String word) {
        kafkaProducer.sendMessage(word);
    }
}
