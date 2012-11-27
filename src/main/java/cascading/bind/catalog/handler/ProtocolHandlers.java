/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class ProtocolHandlers<Protocol, Format> implements Serializable
  {
  public static final ProtocolHandlers EMPTY = new ProtocolHandlers();

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

  public void add( ProtocolHandler<Protocol, Format> handler )
    {
    handlers.add( handler );
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
  }
