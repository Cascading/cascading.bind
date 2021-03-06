/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.bind.json;

import java.io.IOException;
import java.lang.reflect.Type;

import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 *
 */
public class FieldsDeserializer extends StdDeserializer<Fields>
  {
  public FieldsDeserializer()
    {
    super( Fields.class );
    }

  @Override
  public Fields deserialize( JsonParser jsonParser, DeserializationContext ctxt ) throws IOException, JsonProcessingException
    {
    ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
    ObjectNode root = mapper.readTree( jsonParser );

    Fields fields = Fields.NONE;

    if( root.has( "kind" ) )
      return resolveKind( root.get( "kind" ) );

    JsonNode fieldsNodes = root.get( "names" );
    JsonNode typesNodes = root.get( "types" );

    for( int i = 0; i < fieldsNodes.size(); i++ )
      {
      JsonNode nameNode = fieldsNodes.get( i );
      JsonNode typeNode = typesNodes == null ? null : typesNodes.get( i );

      Comparable name = nameNode.isNumber() ? nameNode.asInt() : nameNode.asText();
      Type type = typeNode != null ? resolveType( typeNode ) : null;

      Fields current;

      if( typeNode != null )
        current = new Fields( name, type );
      else
        current = new Fields( name );

      fields = fields.append( current );
      }

    return fields;
    }

  private Type resolveType( JsonNode typeNode )
    {
    String typeName = typeNode.textValue();

    if( typeName == null )
      throw new IllegalStateException( "type may not be null" );

    return Coercions.asType( typeName );
    }

  private Fields resolveKind( JsonNode node ) throws IOException
    {
    String value = node.textValue();

    if( Fields.NONE.toString().equals( value ) )
      return Fields.NONE;

    if( Fields.UNKNOWN.toString().equals( value ) )
      return Fields.UNKNOWN;

    if( Fields.ALL.toString().equals( value ) )
      return Fields.ALL;

    throw new IllegalStateException( "unknown kind: " + value );
    }
  }
