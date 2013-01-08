/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.bind.catalog;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import cascading.bind.catalog.handler.FormatHandlers;
import cascading.tuple.Fields;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 */
public class JSONTest
  {
  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void test() throws IOException, JsonGenerationException
    {
    Catalog<String, String> wroteCatalog = new Catalog<String, String>();

    FormatHandlers<String, String> handlers = new FormatHandlers<String, String>();
    handlers.add( new TestFormatHandler() );
    handlers.add( new TestFormatHandler() );

    wroteCatalog.addStereotype( new Stereotype<String, String>( "hdfs", "first", new Fields( "one", "two" ).applyTypes( int.class, new TestCoercibleType() ) ) );
    wroteCatalog.addStereotype( new Stereotype<String, String>( "hdfs", "second", Fields.UNKNOWN ) );
    wroteCatalog.addStereotype( new Stereotype<String, String>( handlers, "hdfs", "third", new Fields( "one", "two", "three" ).applyTypes( int.class, Double.class, String.class ) ) );

    String jsonFirst = writeObject( wroteCatalog );

    System.out.println( jsonFirst );

    Catalog firstRead = readCatalog( jsonFirst );

    Assert.assertEquals( wroteCatalog, firstRead );

    String jsonSecond = writeObject( firstRead );

    System.out.println( jsonSecond );

    Catalog secondRead = readCatalog( jsonSecond );

    Assert.assertEquals( firstRead, secondRead );
    }

  private Catalog readCatalog( String json ) throws IOException
    {
    StringReader reader = new StringReader( json );
    return mapper.readValue( reader, Catalog.class );
    }

  private String writeObject( Catalog wroteCatalog ) throws IOException
    {
    StringWriter writer = new StringWriter();
    mapper.writer().withDefaultPrettyPrinter().writeValue( writer, wroteCatalog );

    return writer.toString();
    }
  }
