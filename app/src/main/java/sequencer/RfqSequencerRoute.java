package sequencer;

import org.apache.camel.builder.RouteBuilder;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.StorageType;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class RfqSequencerRoute extends RouteBuilder {

    private final KeyValue kv;

    public RfqSequencerRoute() throws Exception {
        // Connect to NATS
        Connection natsConnection = Nats.connect("nats://localhost:4222");
        KeyValueManagement kvm = natsConnection.keyValueManagement();

        // Create/get KV bucket
        KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
                .name("rfq-dedup")
                .ttl(Duration.ofMinutes(10))
                .storageType(StorageType.Memory)
                .build();

        kvm.create(kvConfig);
        this.kv = natsConnection.keyValue("rfq-dedup");
    }

    @Override
    public void configure() throws Exception {

        from("nats:rfq.raw?servers=nats://localhost:4222")
                .routeId("dedup-sequencer")
                .process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        String body = exchange.getIn().getBody(String.class);

                        // Use SHA-256 hash of the body for deduplication
                        String messageId = Hashing.sha256()
                                .hashString(body, StandardCharsets.UTF_8)
                                .toString();

                        if (kv.keys().contains(messageId)) {
                            // Duplicate message - stop processing
                            System.out.println("Duplicate message detected: " + messageId);
                            exchange.getIn().setHeader("isUnique", false);
                        } else {
                            System.out.println("Unique message detected: " + messageId);
                            // Store the unique message in the KeyValue store
                            kv.put(messageId, body.getBytes(StandardCharsets.UTF_8));
                            // Unique message - continue
                            exchange.getIn().setHeader("isUnique", true);
                        }
                    }
                })
                .log(header("isUnique").isEqualTo(true).toString())
                .filter(header("isUnique").isEqualTo(true))
                .to("nats:rfq.deduped?servers=nats://localhost:4222")
                .end();
    }
}
