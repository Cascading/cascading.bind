/*
 * Copyright (c) 2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.bind.catalog.handler;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ProtocolHandlers<Protocol, Format> extends AbstractList<ProtocolHandler<Protocol, Format>> implements Iterable<ProtocolHandler<Protocol, Format>>, Serializable
  {
  private static final Logger LOG = LoggerFactory.getLogger( ProtocolHandlers.class );

  @JsonProperty
  final List<ProtocolHandler<Protocol, Format>> handlers = new LinkedList<ProtocolHandler<Protocol, Format>>();

  public ProtocolHandlers()
    {
    }

  public ProtocolHandlers( List<ProtocolHandler<Protocol, Format>> handlers )
    {
    this.handlers.addAll( handlers );
    }

  public ProtocolHandlers( ProtocolHandlers<Protocol, Format> handlers )
    {
    this.handlers.addAll( handlers.handlers );
    }

  @Override
  public ProtocolHandler<Protocol, Format> get( int index )
    {
    return handlers.get( index );
    }

  @Override
  public boolean add( ProtocolHandler<Protocol, Format> handler )
    {
    return handlers.add( handler );
    }

  public void addAll( ProtocolHandlers<Protocol, Format> handlers )
    {
    this.handlers.addAll( handlers.handlers );
    }

  public ProtocolHandler<Protocol, Format> findHandlerFor( Protocol protocol )
    {
    for( ProtocolHandler<Protocol, Format> handler : handlers )
      {
      if( handler.handles( protocol ) )
        return handler;
      }

    return null;
    }

  public Set<Protocol> getProtocols()
    {
    Set<Protocol> protocols = new HashSet<Protocol>();

    for( ProtocolHandler<Protocol, Format> handler : handlers )
      {
      Collection<? extends Protocol> currentProtocols = handler.getProtocols();

      if( !Collections.disjoint( protocols, currentProtocols ) )
        LOG.warn( "protocol handler: {} provides one or more duplicate default protocols: {}", handler, currentProtocols );
      else
        protocols.addAll( currentProtocols );
      }

    return protocols;
    }

  public Map<String, List<String>> getProtocolProperties( Protocol protocol )
    {
    for( ProtocolHandler<Protocol, Format> handler : handlers )
      {
      Map<String, List<String>> defaultProperties = handler.getDefaultProperties( protocol );

      if( defaultProperties != null && !defaultProperties.isEmpty() )
        return defaultProperties;
      }

    return Collections.emptyMap();
    }

  @Override
  public Iterator<ProtocolHandler<Protocol, Format>> iterator()
    {
    return handlers.iterator();
    }

  @Override
  public int size()
    {
    return handlers.size();
    }
  }
