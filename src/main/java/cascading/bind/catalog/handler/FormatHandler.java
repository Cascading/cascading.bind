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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import cascading.bind.catalog.Stereotype;
import cascading.scheme.Scheme;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface FormatHandler<Protocol, Format> extends Serializable
  {
  Collection<? extends Format> getFormats();

  boolean handles( Protocol protocol, Format format );

  Scheme createScheme( Stereotype<Protocol, Format> stereotype, Protocol protocol, Format format, Role role );

  Map<String, List<String>> getDefaultProperties( Format format );
  }
