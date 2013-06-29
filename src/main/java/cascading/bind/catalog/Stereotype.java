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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.bind.json.FieldsDeserializer;
import cascading.bind.json.FieldsSerializer;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Class Stereotype is used to map between 'protocols' and 'formats' to available Cascading Scheme instances.
 * <p/>
 * This is particularly useful when creating data processing applications that need to deal with
 * multiple data formats for data (tab delimited, JSON, thrift, etc) and multiple ways to access the data
 * (HDFS, S3, JDBC, Memcached, etc).
 * <p/>
 * This class is type parameterized for P and F where P represent a 'protocol' and F represents
 * a file or data 'format'. Typically P and F are of type {@link Enum}, but may be any standard class.
 * <p/>
 * It is a common practice to sub-class Stereotype so that each new class represents a particular abstract
 * data type like 'person' or an Apache server log record.
 *
 * @param <Protocol> a 'protocol' type
 * @param <Format>   a data 'format' type
 */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.NONE,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Stereotype<Protocol, Format> implements Serializable
  {
  @JsonProperty
  String name = getClass().getSimpleName().replaceAll( "Stereotype$", "" );
  @JsonProperty
  Protocol defaultProtocol;
  @JsonProperty
  Format defaultFormat;
  @JsonSerialize(using = FieldsSerializer.class)
  @JsonDeserialize(using = FieldsDeserializer.class)
  Fields fields;

  @JsonIgnore
  final Map<Point<Protocol, Format>, Scheme> staticSchemes = new HashMap<Point<Protocol, Format>, Scheme>();

  protected Stereotype()
    {
    }

  protected Stereotype( Protocol defaultProtocol )
    {
    this( defaultProtocol, null, null, null );
    }

  protected Stereotype( Protocol defaultProtocol, Format defaultFormat )
    {
    this( defaultProtocol, defaultFormat, null, null );
    }

  public Stereotype( String name, Fields fields )
    {
    this( Collections.EMPTY_MAP, null, null, name, fields );
    }

  @JsonCreator
  public Stereotype( @JsonProperty("defaultProtocol") Protocol defaultProtocol, @JsonProperty("defaultFormat") Format defaultFormat, @JsonProperty("name") String name, @JsonProperty("fields") Fields fields )
    {
    this( Collections.EMPTY_MAP, defaultProtocol, defaultFormat, name, fields );
    }

  /**
   * Simple copy constructor allowing change of stereotype name.
   *
   * @param stereotype
   * @param name
   */
  public Stereotype( Stereotype<Protocol, Format> stereotype, String name )
    {
    this( stereotype.staticSchemes, stereotype.getDefaultProtocol(), stereotype.getDefaultFormat(), name, stereotype.getFields() );
    }

  protected Stereotype( Map<Point<Protocol, Format>, Scheme> staticSchemes, Protocol defaultProtocol, Format defaultFormat, String name, Fields fields )
    {
    this.staticSchemes.putAll( staticSchemes );
    this.defaultProtocol = defaultProtocol;
    this.defaultFormat = defaultFormat;
    this.name = name == null ? this.name : name;
    this.fields = normalize( fields );

    if( this.name == null || this.name.isEmpty() )
      throw new IllegalArgumentException( "name may not be null or empty" );
    }

  public String getName()
    {
    return name;
    }

  protected void setName( String name )
    {
    this.name = name;
    }

  public Protocol getDefaultProtocol()
    {
    return defaultProtocol;
    }

  public Format getDefaultFormat()
    {
    return defaultFormat;
    }

  public Fields getFields()
    {
    return fields;
    }

  public Collection<Format> getAllFormats()
    {
    Set<Format> formats = new HashSet<Format>();

    addAllFormats( formats, staticSchemes.keySet() );

    return formats;
    }

  private void addAllFormats( Set<Format> formats, Set<Point<Protocol, Format>> points )
    {
    for( Point<Protocol, Format> point : points )
      formats.add( point.format );
    }

  public Collection<Protocol> getAllProtocols()
    {
    Set<Protocol> protocols = new HashSet<Protocol>();

    addAllProtocols( protocols, staticSchemes.keySet() );

    return protocols;
    }

  private void addAllProtocols( Set<Protocol> protocols, Set<Point<Protocol, Format>> points )
    {
    for( Point<Protocol, Format> point : points )
      protocols.add( point.protocol );
    }

  private void setFields( Scheme scheme )
    {
    if( scheme.isSource() )
      {
      Fields sourceFields = normalize( scheme.getSourceFields() );

      if( fields == null )
        fields = sourceFields;
      else if( !fields.equals( sourceFields ) )
        throw new IllegalArgumentException( "all schemes added to stereotype must have the same source fields, expected: " + fields + ", received: " + sourceFields + " in stereotype: " + getName() );
      }

    if( scheme.isSink() )
      {
      Fields sinkFields = normalize( scheme.getSinkFields() );

      if( fields == null )
        fields = sinkFields;
      else if( !fields.equals( sinkFields ) )
        throw new IllegalArgumentException( "all schemes added to stereotype must have the same sink fields, expected: " + fields + ", received: " + sinkFields + " in stereotype: " + getName() );
      }
    }

  public void addSchemeFor( Protocol protocol, Format format, Scheme scheme )
    {
    if( protocol == null )
      protocol = defaultProtocol;

    setFields( scheme );

    staticSchemes.put( new Point<Protocol, Format>( protocol, format ), scheme );
    }

  public void addSchemeFor( Format format, Scheme scheme )
    {
    addSchemeFor( null, format, scheme );
    }

  public Scheme getSchemeFor( Protocol protocol, Format format )
    {
    if( protocol == null )
      protocol = defaultProtocol;

    Point<Protocol, Format> pair = pair( protocol, format );

    return staticSchemes.get( pair );
    }

  public Scheme getSchemeFor( Format format )
    {
    return getSchemeFor( null, format );
    }

  protected Point<Protocol, Format> pair( Resource<Protocol, Format, ?> resource )
    {
    Protocol protocol = resource.getProtocol();

    if( protocol == null )
      protocol = defaultProtocol;

    return new Point<Protocol, Format>( protocol, resource.getFormat() );
    }

  protected Point<Protocol, Format> pair( Protocol protocol, Format format )
    {
    if( protocol == null )
      protocol = defaultProtocol;

    return new Point<Protocol, Format>( protocol, format );
    }

  private Fields normalize( Fields fields )
    {
    if( fields != null && fields.equals( Fields.ALL ) )
      fields = Fields.UNKNOWN;

    return fields;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Stereotype that = (Stereotype) object;

    if( defaultProtocol != null ? !defaultProtocol.equals( that.defaultProtocol ) : that.defaultProtocol != null )
      return false;
    if( fields != null ? !fields.equals( that.fields ) : that.fields != null )
      return false;
    if( name != null ? !name.equals( that.name ) : that.name != null )
      return false;
    if( staticSchemes != null ? !staticSchemes.equals( that.staticSchemes ) : that.staticSchemes != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + ( defaultProtocol != null ? defaultProtocol.hashCode() : 0 );
    result = 31 * result + ( fields != null ? fields.hashCode() : 0 );
    result = 31 * result + ( staticSchemes != null ? staticSchemes.hashCode() : 0 );
    return result;
    }
  }
