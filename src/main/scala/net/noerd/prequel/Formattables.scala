package net.noerd.prequel

import java.util.{Locale, Date}

import org.joda.time._
import java.sql.{Time, SQLException}

/**
 * Wrap your optional value in NullComparable to compare with null if None. 
 *
 * Note: The '=' operator is added during formatting so don't include it in your SQL
 */    
class NullComparable( val value: Option[ Formattable ] ) extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = {
        value.map( "=" + _.escaped( formatter) ).getOrElse( "is null" )
    }
    override def addTo( statement: ReusableStatement ): Unit = {
        sys.error( "incompatible with prepared statements" )
    }
}
object NullComparable {
    def apply( value: Option[ Formattable ] ) = new NullComparable( value )
}

/**
 * Wrap your optional value in Nullable to have it converted to null if None
 */
class Nullable( val value: Option[ Formattable ] ) extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = {
        value.map( _.escaped( formatter ) ).getOrElse( "null" )
    }
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addNull
    }
}
object Nullable {
    def apply( value: Option[ Formattable ] ) = new Nullable( value )
}

/**
 * Wrap a parameter string in an Identifier to avoid escaping
 */ 
class Identifier( val value: String ) extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = {
        value
    }
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addString( value )
    }
}
object Identifier {
    def apply( value: String ) = new Identifier( value )
}

//
// String
//
class StringFormattable( val value: String ) extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = {
        formatter.toSQLString( value ) 
    }
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addString( value )
    }
}
object StringFormattable{
    def apply( value: String ) = new StringFormattable( value )
}

//
// NString
//
class NStringFormattable( val value: String ) extends Formattable {
  override def escaped( formatter: SQLFormatter ): String = {
    formatter.toSQLString( value )
  }
  override def addTo( statement: ReusableStatement ): Unit = {
    statement.addString( value )
  }
}
object NStringFormattable{
  def apply( value: String ) = new StringFormattable( value )
}

//
// Boolean
// 
class BooleanFormattable( val value: Boolean ) extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = value.toString
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addBoolean( value )
    }
}
object BooleanFormattable {
    def apply( value: Boolean ) = new BooleanFormattable( value )
}

//
// Long
//
class LongFormattable( val value: Long ) extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = value.toString
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addLong( value )
    }
}
object LongFormattable{
    def apply( value: Long ) = new LongFormattable( value )
}

//
// Int
//
class IntFormattable( val value: Int ) extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = value.toString
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addInt( value )
    }
}
object IntFormattable{
    def apply( value: Int ) = new IntFormattable( value )
}

//
// Float
//
class FloatFormattable( val value: Float ) extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = "%f".formatLocal(Locale.ENGLISH, value )
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addFloat( value )
    }
}
object FloatFormattable{
    def apply( value: Float ) = new FloatFormattable( value )
}

//
// Double
//
class DoubleFormattable( val value: Double ) extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = "%f".formatLocal(Locale.ENGLISH,  value )
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addDouble( value )
    }
}
object DoubleFormattable{
    def apply( value: Double ) = new DoubleFormattable( value )
}

//
// BigDecimal
//
class BigDecimalFormattable( val value: BigDecimal ) extends Formattable {
  override def escaped( formatter: SQLFormatter ): String = value.toString()
  override def addTo( statement: ReusableStatement ): Unit = {
    statement.addBigDecimal( value )
  }
}
object BigDecimalFormattable{
  def apply( value: BigDecimal ) = new BigDecimalFormattable( value )
}


//
// DateTime
//
class DateTimeFormattable( val value: DateTime )
extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = {
        formatter.toSQLString( formatter.timeStampFormatter.print( value ) )
    }
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addDateTime( value )
    }
}

object DateTimeFormattable{
    def apply( value: DateTime ) = {
        new DateTimeFormattable( value )
    }
    def apply( value: Date) = {
        new DateTimeFormattable( new DateTime( value ) )
    }
    def apply( value: LocalDate) = {
      new DateTimeFormattable( value.toDateTimeAtStartOfDay )
    }
    def apply( value: LocalDateTime) = {
      new DateTimeFormattable( value.toDateTime )
    }
}

//
// LocalDateTime
//
class LocalDateTimeFormattable( val value: LocalDateTime )
  extends Formattable {
  override def escaped( formatter: SQLFormatter ): String = {
    formatter.toSQLString( formatter.timeStampFormatter.print( value ) )
  }
  override def addTo( statement: ReusableStatement ): Unit = {
    statement.addDateTime( value.toDateTime )
  }
}
object LocalDateTimeFormattable{
  def apply( value: LocalDateTime ) = {
    new LocalDateTimeFormattable( value )
  }
  def apply( value: Date) = {
    new LocalDateTimeFormattable( new LocalDateTime( value ) )
  }
  def apply( value: LocalDate) = {
    new LocalDateTimeFormattable( value.toLocalDateTime(LocalTime.MIDNIGHT) )
  }
}


//
// LocalDate
//
class LocalDateFormattable( val value: LocalDate )
  extends Formattable {
  override def escaped( formatter: SQLFormatter ): String = {
    formatter.toSQLString( formatter.dateFormatter.print( value ) )
  }
  override def addTo( statement: ReusableStatement ): Unit = {
    statement.addLocalDate( value )
  }
}
object LocalDateFormattable{
  def apply( value: LocalDate ) = {
    new LocalDateFormattable( value )
  }
  def apply( value: Date) = {
    new LocalDateFormattable( new LocalDate( value ) )
  }
  def apply( value: LocalDateTime) = {
    new LocalDateFormattable( value.toLocalDate )
  }
}

//
// LocalTime
//
class TimeFormattable( val value: LocalTime )
  extends Formattable {
  override def escaped( formatter: SQLFormatter ): String = {
    formatter.toSQLString( formatter.timeFormatter.print( value ) )
  }
  override def addTo( statement: ReusableStatement ): Unit = {
    statement.addLocalTime( value )
  }
}

object TimeFormattable{
  def apply( value: LocalTime ) = {
    new TimeFormattable( value.withMillisOfSecond(0) )
  }
  def apply( value: Date) = {
    new TimeFormattable( new LocalTime( value ) )
  }
  def apply( value: LocalDateTime) = {
    new TimeFormattable( value.toLocalTime )
  }
  def apply( value: DateTime) = {
    new TimeFormattable( value.toLocalTime )
  }
}

//
// Duration
//
/**
 * Formats an Duration object by converting it to milliseconds.
 */
class DurationFormattable( val value: Duration ) 
extends Formattable {
    override def escaped( formatter: SQLFormatter ): String = value.getMillis.toString
    override def addTo( statement: ReusableStatement ): Unit = {
        statement.addLong( value.getMillis )
    }
}
object DurationFormattable{
    def apply( value: Duration ) = new DurationFormattable( value )
}

//
// Identity replacement: leaves '?' unchanged when formatting an SQL string
// Not for use when actually running a query!
//
protected class BindFormattable extends Formattable {
  val value = "?"
  override def escaped( formatter: SQLFormatter ): String = value
  override def addTo( statement: ReusableStatement ): Unit = {
    throw new SQLException("Cannot add BindFormattable to a statement")
  }
}
object BindFormattable{
  def apply() = new BindFormattable()
}