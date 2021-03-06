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

package cascading.bind;

import java.util.Properties;

import cascading.bind.catalog.Resource;
import cascading.bind.process.FlowFactory;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;

/**
 * A mock FlowFactory that creates a working Flow for conversion of data between
 * two formats and across multiple protocols.
 */
public class TestCopyFactory extends FlowFactory
  {
  public TestCopyFactory( String name )
    {
    this( null, name );
    }

  public TestCopyFactory( Properties properties, String name )
    {
    super( properties, name );

    getProtocolHandlers( null ).add( new ConversionHandler() );
    setSourceStereotype( name, new CopyStereotype() );
    setSinkStereotype( name, new CopyStereotype() );
    }

  public void addSourceResource( Resource resource )
    {
    addSourceResource( getName(), resource );
    }

  public void addSinkResource( Resource resource )
    {
    addSinkResource( getName(), resource );
    }

  @Override
  protected FlowConnector getFlowConnector()
    {
    return new LocalFlowConnector( getProperties() );
    }

  @Override
  public Flow create()
    {
    FlowDef flowDef = FlowDef.flowDef().setName( getName() );

    return create( flowDef );
    }

  @Override
  public Flow create( FlowDef flowDef )
    {
    Pipe pipe = new Pipe( getName() ); // this forces pipe-lining between the source and sink

    return createFlowFrom( flowDef, pipe );
    }
  }
