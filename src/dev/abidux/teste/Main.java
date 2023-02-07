package dev.abidux.teste;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonReader;

public class Main {

    public static final TesteCodec TESTE_CODEC = new TesteCodec();

    public static void main(String[] args) {
        CodecRegistry defaultCodecRegistry = MongoClient.getDefaultCodecRegistry();
        CodecRegistry registry = CodecRegistries.fromRegistries(defaultCodecRegistry,
                CodecRegistries.fromCodecs(TESTE_CODEC));
        MongoClientOptions options = MongoClientOptions.builder().codecRegistry(registry).build();
        MongoClient mongo = new MongoClient("localhost:27017", options);

        MongoDatabase db = mongo.getDatabase("teste");
        MongoCollection<Document> collection = db.getCollection("colecao");
        if (collection.countDocuments() == 0) {
            Document dc = new Document();
            dc.put("teste", new Teste("aaa"));
            collection.insertOne(dc);
        }

        for (Document document : collection.find()) {
            System.out.println(document);

            Document dc = (Document) document.get("teste");
            Teste teste = TESTE_CODEC.decode(new JsonReader(dc.toJson()), DecoderContext.builder().build());

            System.out.println(teste.text);
            collection.deleteOne(document);
        }

        mongo.close();
    }

    static class Teste {
        public final String text;
        public Teste(String text) {
            this.text = text;
        }
    }

    static class TesteCodec implements Codec<Teste> {
        @Override
        public Teste decode(BsonReader bsonReader, DecoderContext decoderContext) {
            bsonReader.readStartDocument();
            Teste teste = new Teste(bsonReader.readString().substring("MT TOP ".length()));
            bsonReader.readEndDocument();
            return teste;
        }

        @Override
        public void encode(BsonWriter bsonWriter, Teste teste, EncoderContext encoderContext) {
            bsonWriter.writeStartDocument();
            bsonWriter.writeName("teste");
            bsonWriter.writeString("MT TOP super teste");
            bsonWriter.writeEndDocument();
        }

        @Override
        public Class<Teste> getEncoderClass() {
            return Teste.class;
        }
    }

}