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

import java.lang.reflect.Type;

import cascading.tuple.type.CoercibleType;

/**
 *
 */
public class TestCoercibleType implements CoercibleType<String>
  {
  @Override
  public String canonical( Object value )
    {
    return (String) value;
    }

  @Override
  public <Coerce> Coerce coerce( Object value, Type to )
    {
    return (Coerce) value.toString();
    }

  @Override
  public int hashCode()
    {
    return super.hashCode();
    }

  @Override
  public boolean equals( Object obj )
    {
    return obj instanceof TestCoercibleType;
    }
  }
