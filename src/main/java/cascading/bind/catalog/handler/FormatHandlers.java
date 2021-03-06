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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FormatHandlers<Protocol, Format> extends AbstractList<FormatHandler<Protocol, Format>> implements Iterable<FormatHandler<Protocol, Format>>, Serializable
  {
  private static final Logger LOG = LoggerFactory.getLogger( FormatHandlers.class );

  final List<FormatHandler<Protocol, Format>> handlers = new LinkedList<FormatHandler<Protocol, Format>>();

  public FormatHandlers()
    {
    }

  public FormatHandlers( List<FormatHandler<Protocol, Format>> handlers )
    {
    this.handlers.addAll( handlers );
    }

  public FormatHandlers( FormatHandlers<Protocol, Format> handlers )
    {
    this.handlers.addAll( handlers.handlers );
    }

  @Override
  public FormatHandler<Protocol, Format> get( int index )
    {
    return handlers.get( index );
    }

  @Override
  public boolean add( FormatHandler<Protocol, Format> handler )
    {
    return handlers.add( handler );
    }

  public void addAll( FormatHandlers<Protocol, Format> handlers )
    {
    this.handlers.addAll( handlers.handlers );
    }

  public FormatHandler<Protocol, Format> findHandlerFor( Protocol protocol, Format format )
    {
    for( FormatHandler<Protocol, Format> handler : handlers )
      {
      if( handler.handles( protocol, format ) )
        return handler;
      }

    return null;
    }

  public Set<Format> getFormats()
    {
    Set<Format> formats = new HashSet<Format>();

    for( FormatHandler<Protocol, Format> handler : handlers )
      {
      Collection<? extends Format> currentFormats = handler.getFormats();

      if( !Collections.disjoint( formats, currentFormats ) )
        LOG.warn( "format handler: {} provides one or more duplicate default formats: {}", handler, currentFormats );
      else
        formats.addAll( currentFormats );
      }

    return formats;
    }

  public Map<String, List<String>> getFormatProperties( Format format )
    {
    for( FormatHandler<Protocol, Format> handler : handlers )
      {
      Map<String, List<String>> defaultProperties = handler.getDefaultProperties( format );

      if( defaultProperties != null && !defaultProperties.isEmpty() )
        return defaultProperties;
      }

    return Collections.emptyMap();
    }

  @Override
  public Iterator<FormatHandler<Protocol, Format>> iterator()
    {
    return handlers.iterator();
    }

  @Override
  public int size()
    {
    return handlers.size();
    }
  }
