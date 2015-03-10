/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.serializers;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;

public class KafkaSpecificAvroDecoder<T> extends AbstractKafkaSpecificAvroDeserializer<T> implements Decoder<T> {
    protected final String TARGET_SCHEMA_ID = "target.schema.id";

    private final Schema targetSchema;

    public KafkaSpecificAvroDecoder(SchemaRegistryClient schemaRegistry, Schema targetSchema) {
        this.schemaRegistry = schemaRegistry;
        this.targetSchema = targetSchema;
    }

    /**
     * Constructor used by Kafka consumer.
     */
    public KafkaSpecificAvroDecoder(VerifiableProperties props) {
        if (props == null) {
            throw new ConfigException("Missing properties.");
        }
        String url = props.getProperty(SCHEMA_REGISTRY_URL);
        if (url == null) {
            throw new ConfigException("Missing schema registry url!");
        }
        int maxSchemaObject = props.getInt(MAX_SCHEMAS_PER_SUBJECT, DEFAULT_MAX_SCHEMAS_PER_SUBJECT);
        schemaRegistry = new CachedSchemaRegistryClient(url, maxSchemaObject);

        int targetSchemaID = props.getInt(TARGET_SCHEMA_ID, Integer.MIN_VALUE);
        if (targetSchemaID == Integer.MIN_VALUE) {
            throw new ConfigException("Missing schema registry url!");
        }

        try {
            targetSchema = schemaRegistry.getByID(Integer.valueOf(targetSchemaID));
        } catch (IOException e) {
            throw new ConfigException("Unable to fetch target schema: " + targetSchemaID);
        } catch (RestClientException e) {
            throw new ConfigException("Unable to fetch target schema: " + targetSchemaID);
        }
    }

    @Override
    public T fromBytes(byte[] bytes) {
        return deserialize(bytes);
    }

    @Override
    protected Schema getTargetSchema() {
        return targetSchema;
    }
}
