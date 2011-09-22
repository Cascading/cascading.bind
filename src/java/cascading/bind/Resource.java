/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package cascading.bind;

/**
 * Class Resource represents an referenceable and identifiable data resource that has a 'protocol', 'format', and optionally a access 'mode'
 * associated with it.
 *
 * @param <P> a 'protocol' type
 * @param <F> a data 'format' type
 * @param <M> a access 'mode' type
 * @see TapResource
 */
public class Resource<P, F, M>
  {
  private String identifier;
  private P protocol;
  private F format;
  private M mode;

  protected Resource()
    {
    }

  public Resource( String identifier, P protocol, F format, M mode )
    {
    this.identifier = identifier;
    this.protocol = protocol;
    this.format = format;
    this.mode = mode;
    }

  public Resource( String identifier, F format )
    {
    this.identifier = identifier;
    this.format = format;
    }

  public Resource( String identifier, F format, M mode )
    {
    this.identifier = identifier;
    this.format = format;
    this.mode = mode;
    }

  public String getIdentifier()
    {
    return identifier;
    }

  public P getProtocol()
    {
    return protocol;
    }

  public F getFormat()
    {
    return format;
    }

  public M getMode()
    {
    return mode;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Resource ) )
      return false;

    Resource resource = (Resource) object;

    if( format != null ? !format.equals( resource.format ) : resource.format != null )
      return false;
    if( identifier != null ? !identifier.equals( resource.identifier ) : resource.identifier != null )
      return false;
    if( mode != null ? !mode.equals( resource.mode ) : resource.mode != null )
      return false;
    if( protocol != null ? !protocol.equals( resource.protocol ) : resource.protocol != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = identifier != null ? identifier.hashCode() : 0;
    result = 31 * result + ( protocol != null ? protocol.hashCode() : 0 );
    result = 31 * result + ( format != null ? format.hashCode() : 0 );
    result = 31 * result + ( mode != null ? mode.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( "Resource" );
    sb.append( "{identifier='" ).append( identifier ).append( '\'' );
    sb.append( ", protocol=" ).append( protocol );
    sb.append( ", format=" ).append( format );
    sb.append( ", mode=" ).append( mode );
    sb.append( '}' );
    return sb.toString();
    }
  }
