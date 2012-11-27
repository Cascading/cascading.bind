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

import cascading.bind.catalog.Point;

/**
 *
 */
public class FormatHandlers<Protocol, Format> implements Serializable
  {
  public static final FormatHandlers EMPTY = new FormatHandlers();

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

  public void add( FormatHandler<Protocol, Format> handler )
    {
    handlers.add( handler );
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
  }
