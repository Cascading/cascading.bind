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

package cascading.bind;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Resource;
import cascading.bind.catalog.handler.ProtocolHandler;
import cascading.bind.tap.HTTPTap;
import cascading.bind.tap.JDBCScheme;
import cascading.bind.tap.JDBCTap;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;

/** A mock resource that acts as a factory for specific Tap types. */
public class ConversionHandler implements ProtocolHandler<Protocol, Format>
  {
  @Override
  public Tap createTap( Scheme scheme, Resource<Protocol, Format, SinkMode> resource )
    {
    Protocol protocol = resource.getProtocol();

    if( protocol == null )
      protocol = Protocol.FILE;

    switch( protocol )
      {
      case FILE:
        return new FileTap( scheme, resource.getIdentifier(), resource.getMode() );
      case JDBC:
        return new JDBCTap( (JDBCScheme) scheme, resource.getIdentifier(), resource.getMode() );
      case HTTP:
        return new HTTPTap( scheme, resource.getIdentifier(), resource.getMode() );
      }

    throw new IllegalStateException( "no tap for given protocol: " + resource.getProtocol() );
    }

  @Override
  public boolean handles( Protocol protocol )
    {
    return true;
    }

  @Override
  public Map<String, List<String>> getDefaultProperties( Protocol protocol )
    {
    return null;
    }

  @Override
  public Collection<? extends Protocol> getProtocols()
    {
    return Arrays.asList( Protocol.FILE, Protocol.JDBC, Protocol.HTTP );
    }
  }
