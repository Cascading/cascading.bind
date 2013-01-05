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

package cascading.bind.process;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.bind.catalog.Resource;
import cascading.bind.catalog.Stereotype;
import cascading.bind.catalog.handler.ProtocolHandler;
import cascading.bind.catalog.handler.ProtocolHandlers;
import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tap.MultiSinkTap;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

/**
 * Class FlowFactory is a sub-class of {@link ProcessFactory} that returns Cascading {@link Flow} instances.
 * <p/>
 * This class should be sub-classed when the intent is to create new Cascading {@link Flow} instances.
 * It provides convenience methods for {@link Tap} and {@link Flow} instantiation.
 */
public abstract class FlowFactory<Protocol, Format> extends ProcessFactory<Flow, Resource<Protocol, Format, SinkMode>>
  {
  protected String name;

  protected ProtocolHandlers<Protocol, Format> protocolHandlers = new ProtocolHandlers<Protocol, Format>();

  protected FlowFactory()
    {
    }

  protected FlowFactory( Properties properties, ProtocolHandlers<Protocol, Format> protocolHandlers, String name )
    {
    super( properties );
    this.protocolHandlers = protocolHandlers;
    this.name = name;
    }

  protected FlowFactory( Properties properties, String name )
    {
    super( properties );
    this.name = name;
    }

  public String getName()
    {
    return name;
    }

  public ProtocolHandlers<Protocol, Format> getProtocolHandlers()
    {
    return protocolHandlers;
    }

  /**
   * Method getSourceTapFor returns a new {@link Tap} instance for the given name.
   * <p/>
   * First the bound {@link cascading.bind.catalog.Stereotype} is looked up, then a Tap is created by the bound
   * {@link cascading.bind.catalog.handler.ProtocolHandler} instance.
   * <p/>
   * If more than one Resource is bound to the given name, a {@link MultiSourceTap}
   * will be returned encapsulating all the resulting Tap instances.
   *
   * @param sourceName
   * @return
   */
  public Tap getSourceTapFor( String sourceName )
    {
    Stereotype stereotype = getSourceStereotype( sourceName );

    if( stereotype == null )
      throw new IllegalArgumentException( "could not find stereotype for source name: " + sourceName );

    return getSourceTapFor( sourceName, stereotype );
    }

  protected Tap getSourceTapFor( String sourceName, Stereotype<Protocol, Format> stereotype )
    {
    List<Resource<Protocol, Format, SinkMode>> resources = getSourceResources( sourceName );

    Tap[] taps = createTapFor( stereotype, resources );

    if( taps == null )
      return null;

    if( taps.length == 1 )
      return taps[ 0 ];

    return new MultiSourceTap( taps );
    }

  /**
   * Method getSinkTapFor returns a new {@link Tap} instance for the given name.
   * <p/>
   * First the bound {@link cascading.bind.catalog.Stereotype} is looked up, then a Tap is created by the bound
   * {@link cascading.bind.catalog.handler.ProtocolHandler} instance.
   * <p/>
   * If more than one Resource is bound to the given name, a {@link MultiSinkTap}
   * will be returned encapsulating all the resulting Tap instances.
   *
   * @param sinkName
   * @return
   */
  public Tap getSinkTapFor( String sinkName )
    {
    Stereotype stereotype = getSinkStereotype( sinkName );

    if( stereotype == null )
      throw new IllegalArgumentException( "could not find stereotype for sink name: " + sinkName );

    return getSinkTapFor( sinkName, stereotype );
    }

  protected Tap getSinkTapFor( String sinkName, Stereotype<Protocol, Format> stereotype )
    {
    List<Resource<Protocol, Format, SinkMode>> resources = getSinkResources( sinkName );

    Tap[] taps = createTapFor( stereotype, resources );

    if( taps == null )
      return null;

    if( taps.length == 1 )
      return taps[ 0 ];

    return new MultiSinkTap( taps );
    }

  ProtocolHandler getTapHandler( Protocol protocol )
    {
    return protocolHandlers.findHandlerFor( protocol );
    }

  private Tap[] createTapFor( Stereotype<Protocol, Format> stereotype, List<Resource<Protocol, Format, SinkMode>> resources )
    {
    if( resources.isEmpty() )
      return null;

    Tap[] taps = new Tap[ resources.size() ];

    for( int i = 0; i < resources.size(); i++ )
      {
      Resource<Protocol, Format, SinkMode> resource = resources.get( i );
      Protocol protocol = resource.getProtocol();

      if( protocol == null )
        protocol = stereotype.getDefaultProtocol();

      ProtocolHandler protocolHandler = getTapHandler( protocol );

      if( protocolHandler == null )
        throw new IllegalStateException( "could not find handler for protocol: " + protocol );

      taps[ i ] = protocolHandler.createTap( stereotype, resource );
      }
    return taps;
    }

  protected Tap[] getSourceTapsFor( String... sourceNames )
    {
    Tap[] taps = new Tap[ sourceNames.length ];

    for( int i = 0; i < sourceNames.length; i++ )
      {
      taps[ i ] = getSourceTapFor( sourceNames[ i ] );

      if( taps[ i ] == null )
        throw new IllegalArgumentException( "no resource found for source name: " + sourceNames[ i ] );
      }

    return taps;
    }

  protected Tap[] getSinkTapsFor( String... sinkNames )
    {
    Tap[] taps = new Tap[ sinkNames.length ];

    for( int i = 0; i < sinkNames.length; i++ )
      {
      taps[ i ] = getSinkTapFor( sinkNames[ i ] );

      if( taps[ i ] == null )
        throw new IllegalArgumentException( "no resource found for sink name: " + sinkNames[ i ] );
      }

    return taps;
    }

  protected Map<String, Tap> getSourceTapsMap( Pipe... sinkPipes )
    {
    Set<Pipe> sourcePipesSet = new HashSet<Pipe>();

    for( Pipe pipe : sinkPipes )
      Collections.addAll( sourcePipesSet, pipe.getHeads() );

    Pipe[] sourcePipes = sourcePipesSet.toArray( new Pipe[ sourcePipesSet.size() ] );
    String[] names = makeNames( sourcePipes );

    return Cascades.tapsMap( sourcePipes, getSourceTapsFor( names ) );
    }

  protected Map<String, Tap> getSinkTapsMap( Pipe... sinkPipes )
    {
    String[] names = makeNames( sinkPipes );

    return Cascades.tapsMap( sinkPipes, getSinkTapsFor( names ) );
    }

  private String[] makeNames( Pipe[] pipes )
    {
    String[] names = new String[ pipes.length ];

    for( int i = 0; i < pipes.length; i++ )
      names[ i ] = pipes[ i ].getName();

    return names;
    }

  /**
   * Method getFlowConnector returns a new {@link FlowConnector} instance using
   * the parent class {@code properties} values.
   *
   * @return
   */
  protected abstract FlowConnector getFlowConnector();

  /**
   * Method createFlowFrom is a convenience method that returns a new {@link Flow} instance.
   * <p/>
   * After all source and sink resources have been bound, the {@link #create()} implementation
   * should call this method to quickly bind source and sink taps to the given assembly head and tail
   * {@link Pipe} instances.
   *
   * @param name
   * @param tails
   * @return
   */
  protected Flow createFlowFrom( String name, Pipe... tails )
    {
    FlowDef flowDef = FlowDef.flowDef()
      .setName( name )
      .addTails( tails );

    return createFlowFrom( flowDef, tails );
    }

  protected Flow createFlowFrom( FlowDef flowDef, Pipe... tails )
    {
    Map<String, Tap> sources = getSourceTapsMap( tails );
    Map<String, Tap> sinks = getSinkTapsMap( tails );

    flowDef.addSources( sources );
    flowDef.addSinks( sinks );

    return getFlowConnector().connect( flowDef );
    }
  }
