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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import cascading.tuple.Fields;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonValue;

/** Class Stereotypes maintains a collection of {@link Stereotype} instances for lookup. */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.NONE,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Stereotypes<Protocol, Format> implements Serializable
  {
  static class InsensitiveMap<V> extends TreeMap<String, V>
    {
    public InsensitiveMap()
      {
      super( String.CASE_INSENSITIVE_ORDER );
      }

    @Override
    public V get( Object key )
      {
      return super.get( key.toString().toLowerCase() );
      }

    @Override
    public V put( String key, V value )
      {
      return super.put( key.toLowerCase(), value );
      }
    }

  Map<String, Stereotype<Protocol, Format>> nameToStereotype = new InsensitiveMap<Stereotype<Protocol, Format>>();
  Map<Fields, Stereotype<Protocol, Format>> fieldsToStereotype = new HashMap<Fields, Stereotype<Protocol, Format>>();

  public Stereotypes()
    {
    }

  @JsonCreator
  public Stereotypes( Collection<Stereotype<Protocol, Format>> stereotypes )
    {
    setStereotypes( stereotypes );
    }

  public Collection<String> getStereotypeNames()
    {
    return nameToStereotype.keySet();
    }

  @JsonGetter
  @JsonValue
  public Collection<Stereotype<Protocol, Format>> getStereotypes()
    {
    return nameToStereotype.values();
    }

  @JsonSetter
  public void setStereotypes( Collection<Stereotype<Protocol, Format>> stereotypes )
    {
    for( Stereotype<Protocol, Format> stereotype : stereotypes )
      addStereotype( stereotype );
    }

  public void addStereotype( Stereotype<Protocol, Format> stereotype )
    {
    if( nameToStereotype.containsKey( stereotype.getName() ) )
      throw new IllegalArgumentException( "stereotypes already contains stereotype for: " + stereotype.getName() + ", with fields: " + nameToStereotype.get( stereotype.getName() ).getFields() );

    if( fieldsToStereotype.containsKey( stereotype.getFields() ) )
      throw new IllegalArgumentException( "stereotypes already contains stereotype for: " + stereotype.getFields() + ", named: " + fieldsToStereotype.get( stereotype.getFields() ).getName() );

    nameToStereotype.put( stereotype.getName(), stereotype );
    fieldsToStereotype.put( stereotype.getFields(), stereotype );
    }

  public Collection<Format> getAllFormats()
    {
    Set<Format> formats = new HashSet<Format>();

    for( Stereotype<Protocol, Format> stereotype : nameToStereotype.values() )
      formats.addAll( stereotype.getAllFormats() );

    return formats;
    }

  public Collection<Protocol> getAllProtocols()
    {
    Set<Protocol> protocols = new HashSet<Protocol>();

    for( Stereotype<Protocol, Format> stereotype : nameToStereotype.values() )
      protocols.addAll( stereotype.getAllProtocols() );

    return protocols;
    }

  public Stereotype<Protocol, Format> getStereotypeFor( String name )
    {
    if( name == null || name.isEmpty() )
      throw new IllegalArgumentException( "name may not be null" );

    return nameToStereotype.get( name );
    }

  public Stereotype<Protocol, Format> getStereotypeFor( Fields fields )
    {
    return fieldsToStereotype.get( normalize( fields ) );
    }

  public boolean removeStereotype( String name )
    {
    Stereotype<Protocol, Format> stereotype = nameToStereotype.remove( name );

    if( stereotype == null )
      return false;

    return fieldsToStereotype.remove( stereotype.getFields() ) != null;
    }

  public boolean renameStereotype( String name, String newName )
    {
    Stereotype<Protocol, Format> stereotype = nameToStereotype.remove( name );

    if( stereotype == null )
      return false;

    fieldsToStereotype.remove( stereotype.getFields() );

    addStereotype( new Stereotype<Protocol, Format>( stereotype, newName ) );

    return true;
    }

  private Fields normalize( Fields fields )
    {
    if( fields.equals( Fields.ALL ) )
      fields = Fields.UNKNOWN;

    return fields;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "Stereotypes" );
    sb.append( "{nameToStereotype=" ).append( nameToStereotype );
    sb.append( '}' );
    return sb.toString();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Stereotypes stereotypes = (Stereotypes) object;

    if( !nameToStereotype.equals( stereotypes.nameToStereotype ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return nameToStereotype.hashCode();
    }
  }
