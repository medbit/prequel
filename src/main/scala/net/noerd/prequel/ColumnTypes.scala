package net.noerd.prequel

import java.util.Date

import org.joda.time._

//
// String
//
class StringColumnType( row: ResultSetRow ) extends ColumnType[ String ] {
    override def nextValueOption: Option[ String ] = row.nextString
    override def columnValueOption(columnName: String): Option[ String ] = row.columnString(columnName)
}
object StringColumnType extends ColumnTypeFactory[ String ] {
    def apply( row: ResultSetRow ) = new StringColumnType( row )
}

//
// NString
//
class NStringColumnType( row: ResultSetRow ) extends ColumnType[ String ] {
  override def nextValueOption: Option[ String ] = row.nextNString
  override def columnValueOption(columnName: String): Option[ String ] = row.columnNString(columnName)
}
object NStringColumnType extends ColumnTypeFactory[ String ] {
  def apply( row: ResultSetRow ) = new StringColumnType( row )
}

//
// Boolean
// 
class BooleanColumnType( row: ResultSetRow ) extends ColumnType[ Boolean ] {
    override def nextValueOption: Option[ Boolean ] = row.nextBoolean
    override def columnValueOption(columnName: String): Option[ Boolean ] = row.columnBoolean(columnName)
}
object BooleanColumnType extends ColumnTypeFactory[ Boolean ] {
    def apply( row: ResultSetRow ) = new BooleanColumnType( row )
}

//
// Long
//
class LongColumnType( row: ResultSetRow ) extends ColumnType[ Long ] {
    override def nextValueOption: Option[ Long ] = row.nextLong
    override def columnValueOption(columnName: String): Option[ Long ] = row.columnLong(columnName)
}
object LongColumnType extends ColumnTypeFactory[ Long ] {
    def apply( row: ResultSetRow ) = new LongColumnType( row )
}

//
// Int
//
class IntColumnType( row: ResultSetRow ) extends ColumnType[ Int ] {
    override def nextValueOption: Option[ Int ] = row.nextInt
    override def columnValueOption(columnName: String): Option[ Int ] = row.columnInt(columnName)
}
object IntColumnType extends ColumnTypeFactory[ Int ] {
    def apply( row: ResultSetRow ) = new IntColumnType( row )
}

//
// Float
//
class FloatColumnType( row: ResultSetRow ) extends ColumnType[ Float ] {
    override def nextValueOption: Option[ Float ] = row.nextFloat
    override def columnValueOption(columnName: String): Option[ Float ] = row.columnFloat(columnName)
}
object FloatColumnType extends ColumnTypeFactory[ Float ] {
    def apply( row: ResultSetRow ) = new FloatColumnType( row )
}

//
// Double
//
class DoubleColumnType( row: ResultSetRow ) extends ColumnType[ Double ] {
    override def nextValueOption: Option[ Double ] = row.nextDouble
    override def columnValueOption(columnName: String): Option[ Double ] = row.columnDouble(columnName)
}
object DoubleColumnType extends ColumnTypeFactory[ Double ] {
    def apply( row: ResultSetRow ) = new DoubleColumnType( row )
}

//
// BigDecimal
//
class BigDecimalColumnType( row: ResultSetRow ) extends ColumnType[ BigDecimal ] {
  override def nextValueOption: Option[ BigDecimal ] = row.nextBigDecimal.map( d => BigDecimal.javaBigDecimal2bigDecimal( d ) )
  override def columnValueOption(columnName: String): Option[ BigDecimal ] = row.columnBigDecimal(columnName).map( d => BigDecimal.javaBigDecimal2bigDecimal( d ) )
}
object BigDecimalColumnType extends ColumnTypeFactory[ BigDecimal ] {
  def apply( row: ResultSetRow ) = new BigDecimalColumnType( row )
}

//
// DateTime
//
class DateTimeColumnType( row: ResultSetRow ) extends ColumnType[ DateTime ] {
    override def nextValueOption: Option[ DateTime ] = row.nextDate.map( d => new DateTime( d.getTime ) )
    override def columnValueOption(columnName: String): Option[ DateTime ] = row.columnDate(columnName).map( d => new DateTime( d.getTime ) )
}
object DateTimeColumnType extends ColumnTypeFactory[ DateTime ] {
    def apply( row: ResultSetRow ) = new DateTimeColumnType( row )
}
class DateColumnType( row: ResultSetRow ) extends ColumnType[ Date ] {
    override def nextValueOption: Option[ Date ] = row.nextDate
    override def columnValueOption(columnName: String): Option[ Date ] = row.columnDate(columnName)
}
object DateColumnType extends ColumnTypeFactory[ Date ] {
    def apply( row: ResultSetRow ) = new DateColumnType( row )
}
class LocalDateColumnType( row: ResultSetRow ) extends ColumnType[ LocalDate ] {
  override def nextValueOption: Option[ LocalDate ] = row.nextDate.map( d => new LocalDate( d.getTime ))
  override def columnValueOption(columnName: String): Option[ LocalDate ] = row.columnDate(columnName).map( d => new LocalDate( d.getTime ))
}
object LocalDateColumnType extends ColumnTypeFactory[ LocalDate ] {
  def apply( row: ResultSetRow ) = new LocalDateColumnType( row )
}
class LocalTimeColumnType( row: ResultSetRow ) extends ColumnType[ LocalTime ] {
  override def nextValueOption: Option[ LocalTime ] = row.nextTime.map( d => new LocalTime( d.getTime ))
  override def columnValueOption(columnName: String): Option[ LocalTime ] = row.columnDate(columnName).map( d => new LocalTime( d.getTime ))
}
object LocalTimeColumnType extends ColumnTypeFactory[ LocalTime ] {
  def apply( row: ResultSetRow ) = new LocalTimeColumnType( row )
}
class LocalDateTimeColumnType( row: ResultSetRow ) extends ColumnType[ LocalDateTime ] {
  override def nextValueOption: Option[ LocalDateTime ] = row.nextDateTime.map( d => new LocalDateTime( d.getTime ))
  override def columnValueOption(columnName: String): Option[ LocalDateTime ] = row.columnDate(columnName).map( d => new LocalDateTime( d.getTime ))
}
object LocalDateTimeColumnType extends ColumnTypeFactory[ LocalDateTime ] {
  def apply( row: ResultSetRow ) = new LocalDateTimeColumnType( row )
}
//
// Duration
//
class DurationColumnType( row: ResultSetRow ) extends ColumnType[ Duration ] {
    override def nextValueOption: Option[ Duration ] = row.nextLong.map( new Duration( _ ) )
    override def columnValueOption(columnName: String): Option[ Duration ] = row.columnLong(columnName).map( new Duration( _ ) )
}
object DurationColumnType extends ColumnTypeFactory[ Duration ] {
    def apply( row: ResultSetRow ) = new DurationColumnType( row )
}