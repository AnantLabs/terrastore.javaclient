package terrastore.client.connection.resteasy;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import terrastore.client.connection.ErrorMessage;
import terrastore.client.mapping.JsonObjectDescriptor;

public class ErrorMessageDescriptor implements JsonObjectDescriptor<ErrorMessage> {

    @Override
    public JsonDeserializer<ErrorMessage> getJsonDeserializer() {
        return new JsonDeserializer<ErrorMessage>() {

            @Override
            public ErrorMessage deserialize(JsonParser parser, DeserializationContext context) throws IOException, JsonProcessingException {
                String message = null;
                Integer code = null;
                JsonToken currentToken = parser.getCurrentToken();
                do {
                   if (currentToken == JsonToken.FIELD_NAME) { 
                    if (parser.getCurrentName().equals("message")) {
                        parser.nextToken();
                        message = parser.getText();
                    } else if (parser.getCurrentName().equals("code")) {
                        parser.nextToken();
                        code = parser.getIntValue();
                    }
                   }
                } while ((currentToken = parser.nextToken()) != null);
                return new ErrorMessage(message, code);
            }
            
        };
    }

    @Override
    public JsonSerializer<ErrorMessage> getJsonSerializer() {
        return new JsonSerializer<ErrorMessage>() {

            @Override
            public void serialize(ErrorMessage arg0, JsonGenerator arg1, SerializerProvider arg2) throws IOException, JsonProcessingException {
                throw new UnsupportedOperationException("Cannot serialize ErrorMessage");
            }
            
        };
    }

    @Override
    public Class<ErrorMessage> getObjectClass() {
        return ErrorMessage.class;
    }

}
