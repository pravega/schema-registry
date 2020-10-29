package io.pravega.schemaregistry.serializer.avro.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.EncodingId;
import io.pravega.schemaregistry.contract.data.EncodingInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.contract.data.VersionInfo;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.avro.testobjs.generated.avro.AddressEntry;
import io.pravega.schemaregistry.serializer.avro.testobjs.generated.avro.User;
import io.pravega.schemaregistry.serializer.shared.codec.Codecs;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@Slf4j
public class AvroDeserializerTest {

    private Serializer<User> serializer;
    private AvroDeserializer<User> avroDeserializer;

    @Before
    public void init() {
        AvroSchema<User> schema = AvroSchema.of(User.class);
        VersionInfo versionInfo1 = new VersionInfo("avroUser1", 0, 0);
        SchemaRegistryClient client = mock(SchemaRegistryClient.class);
        doAnswer(x -> true).when(client).canReadUsing(anyString(), any());
        doAnswer(x -> new EncodingId(0)).when(client).getEncodingId(anyString(), any(), any());
        doAnswer(x -> new EncodingInfo(versionInfo1, schema.getSchemaInfo(), Codecs.None.getCodec().getCodecType())).when(client).getEncodingInfo(anyString(), eq(new EncodingId(0)));
        SerializerConfig serializerConfig = SerializerConfig.builder().registryClient(client).groupId("avroUser1")
                .createGroup(SerializationFormat.Avro).registerSchema(true).build();
        this.serializer = AvroSerializerFactory
                .serializer(serializerConfig, schema);
        this.avroDeserializer = Mockito.spy((AvroDeserializer<User>)AvroSerializerFactory.deserializer(
                serializerConfig, schema));
    }

    @Test
    public void testCreatingReadersOnceForSchema() {
        User user = User.newBuilder()
                .setUserId("111111111111")
                .setBiography("Greg Egan was born 20 August 1961")
                .setName("Greg Egan")
                .setEventTimestamp(System.currentTimeMillis())
                .setKeyValues(null)
                .setKeyValues2(null)
                .setKeyValues3(null)
                .setAddress(AddressEntry.newBuilder().setCity("Perth")
                        .setPostalCode(5018)
                        .setStreetAddress("4/19 Gardner Road").build()).build();

        ImmutableMap<ByteBuffer, DatumReader<User>> knownSchemaReaders1 =  avroDeserializer.getKnownSchemaReaders();
        Assert.assertFalse(knownSchemaReaders1.isEmpty());
        Assert.assertEquals(1, knownSchemaReaders1.size());
        AvroSchema<User> userAvroSchema = AvroSchema.of(User.class);
        DatumReader<User> datumReader = knownSchemaReaders1.get(userAvroSchema.getSchemaInfo().getSchemaData());
        Assert.assertNotNull(datumReader);

        ByteBuffer serialized = serializer.serialize(user);
        int payloadSize = serialized.limit();
        log.info("serialized into {}", payloadSize);
        Assert.assertEquals(100, payloadSize);
        byte[] bytes = serialized.array();
        log.info("bytes: {}", new String(bytes, StandardCharsets.UTF_8));
        User user1 = avroDeserializer.deserialize(ByteBuffer.wrap(bytes));

        log.info("deserialized {}", user1);
        Assert.assertEquals(user, user1);
        serializer.serialize(user1);
        ImmutableMap<ByteBuffer, DatumReader<User>> knownSchemaReaders2 =  avroDeserializer.getKnownSchemaReaders();
        Assert.assertEquals(1, knownSchemaReaders2.size());
        Assert.assertEquals(knownSchemaReaders1, knownSchemaReaders2);
        // called zero times outside constructor
        Mockito.verify(avroDeserializer, Mockito.times(0)).createDatumReader(Mockito.any(), Mockito.anyBoolean());
    }

}
