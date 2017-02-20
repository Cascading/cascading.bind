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

package cascading.bind.catalog;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.handler.FormatHandler;
import cascading.bind.catalog.handler.Role;
import cascading.scheme.Scheme;
import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 *
 */
@JsonAutoDetect(
  fieldVisibility = JsonAutoDetect.Visibility.NONE,
  getterVisibility = JsonAutoDetect.Visibility.NONE,
  setterVisibility = JsonAutoDetect.Visibility.NONE)
public class TestFormatHandler implements FormatHandler<String, String>
  {

  private List<String> strings = Arrays.asList( "csv", "tsv" );

  public TestFormatHandler()
    {
    }

  @Override
  public Collection<? extends String> getFormats()
    {
    return strings;
    }

  @Override
  public boolean handles( String protocol, String format )
    {
    return true;
    }

  @Override
  public Scheme createScheme( Stereotype<String, String> stereotype, String string, String string1, Role role )
    {
    return null;
    }

  @Override
  public Map<String, List<String>> getDefaultProperties( String string )
    {
    return Collections.EMPTY_MAP;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    TestFormatHandler that = (TestFormatHandler) object;

    if( strings != null ? !strings.equals( that.strings ) : that.strings != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return strings != null ? strings.hashCode() : 0;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "TestFormatHandler" );
    sb.append( "{strings=" ).append( strings );
    sb.append( '}' );
    return sb.toString();
    }
  }
