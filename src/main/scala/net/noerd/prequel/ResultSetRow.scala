package net.noerd.prequel

import java.util.Date

import java.sql.{Time, ResultSet}

import scala.collection.mutable.ArrayBuffer


/**
 * Wraps a ResultSet in a row context. The ResultSetRow gives access
 * to the current row with no possibility to change row. The data of
 * the row can be accessed though the next<Type> methods which return
 * the optional value of the next column.
 */
class ResultSetRow( val rs: ResultSet ) {
    /** Maintain the current position. */
    private var position = 0
      
    def nextBoolean: Option[ Boolean ] = nextValueOption( rs.getBoolean )
    def nextInt: Option[ Int ] = nextValueOption( rs.getInt )
    def nextLong: Option[ Long ] = nextValueOption( rs.getLong )
    def nextFloat: Option[ Float ] = nextValueOption( rs.getFloat )
    def nextDouble: Option[ Double ] = nextValueOption( rs.getDouble )
    def nextBigDecimal: Option[ java.math.BigDecimal ] = nextValueOption( rs.getBigDecimal )
    def nextString: Option[ String ] = nextValueOption( rs.getString )
    def nextNString: Option[ String ] = nextValueOption( rs.getNString )
    def nextDate: Option[ Date ] =  nextValueOption( rs.getTimestamp )
    def nextTime: Option[ Time ] =  nextValueOption( rs.getTime )
    def nextDateTime: Option[ Date ] =  nextValueOption( rs.getTimestamp )
    def nextObject: Option[ AnyRef ] = nextValueOption( rs.getObject )

    def columnBoolean(columnName: String): Option[ Boolean ] = columnValueOption(columnName, rs.getBoolean )
    def columnInt(columnName: String): Option[ Int ] = columnValueOption(columnName, rs.getInt )
    def columnLong(columnName: String): Option[ Long ] = columnValueOption(columnName, rs.getLong )
    def columnFloat(columnName: String): Option[ Float ] = columnValueOption(columnName, rs.getFloat )
    def columnDouble(columnName: String): Option[ Double ] = columnValueOption(columnName, rs.getDouble )
    def columnBigDecimal(columnName: String): Option[ java.math.BigDecimal ] = columnValueOption(columnName, rs.getBigDecimal )
    def columnString(columnName: String): Option[ String ] = columnValueOption(columnName, rs.getString )
    def columnNString(columnName: String): Option[ String ] = columnValueOption(columnName, rs.getNString )
    def columnDate(columnName: String): Option[ Date ] =  columnValueOption(columnName, rs.getTimestamp )
    def columnTime(columnName: String): Option[ Time ] =  columnValueOption(columnName, rs.getTime )
    def columnDateTime(columnName: String): Option[ Date ] =  columnValueOption(columnName, rs.getTimestamp )
    def columnObject(columnName: String): Option[ AnyRef ] = columnValueOption(columnName, rs.getObject )

    def columnNames: Seq[ String ]= {
        val columnNames = ArrayBuffer.empty[ String ]
        val metaData = rs.getMetaData
        for(index <- 0.until( metaData.getColumnCount ) ) {
            columnNames += metaData.getColumnName( index + 1 ).toLowerCase
        }
        columnNames
    }

    private def incrementPosition = {
        position = position + 1 
    }

    private def nextValueOption[T]( f: (Int) => T ): Option[ T ] = {
        incrementPosition
        val value = f( position )
        if( rs.wasNull ) None
        else Some( value )
    }

    def columnValueOption[T](columnName:String, f: (String) => T ): Option[ T ] = {
      val value = f(columnName)
      if( rs.wasNull ) None
      else Some( value )
    }

}

object ResultSetRow {
    
    def apply( rs: ResultSet ): ResultSetRow = {
        new ResultSetRow( rs )
    }
}


case class Column(name: String)

/**
 * Defines a number of implicit conversion methods for the supported ColumnTypes. A call
 * to one of these methods will return the next value of the right type. The methods make
 * it easy to step through a row in order to build an object from it as shown in the example
 * below.
 * 
 * Handles all types supported by Prequel as well as Option variants of those.
 *
 *     import net.noerd.prequel.ResultSetRowImplicits._
 *
 *     case class Person( id: Long, name: String, birthdate: DateTime )
 *
 *     InTransaction { tx =>
 *         tx.select( "select id, name, birthdate from people" ) { r =>
 *             Person( r, r, r )
 *         }
 *     }
 */
object ResultSetRowImplicits {
    implicit def row2Boolean( row: ResultSetRow ) = BooleanColumnType( row ).nextValue
    implicit def row2Int( row: ResultSetRow ): Int = IntColumnType( row ).nextValue
    implicit def row2Long( row: ResultSetRow ): Long = LongColumnType( row ).nextValue
    implicit def row2Float( row: ResultSetRow ) = FloatColumnType( row ).nextValue
    implicit def row2Double( row: ResultSetRow ) = DoubleColumnType( row ).nextValue
    implicit def row2BigDecimal( row: ResultSetRow ) = BigDecimalColumnType( row ).nextValue
    implicit def row2String( row: ResultSetRow ) = StringColumnType( row ).nextValue
    implicit def row2Date( row: ResultSetRow ) = DateColumnType( row ).nextValue
    implicit def row2DateTime( row: ResultSetRow ) = DateTimeColumnType( row ).nextValue
    implicit def row2LocalDate( row: ResultSetRow ) = LocalDateColumnType( row ).nextValue
    implicit def row2LocalTime( row: ResultSetRow ) = LocalTimeColumnType( row ).nextValue
    implicit def row2LocalDateTime( row: ResultSetRow ) = LocalDateTimeColumnType( row ).nextValue
    implicit def row2Duration( row: ResultSetRow ) = DurationColumnType( row ).nextValue

    implicit def row2BooleanOption( row: ResultSetRow ) = BooleanColumnType( row ).nextValueOption
    implicit def row2IntOption( row: ResultSetRow ) = IntColumnType( row ).nextValueOption
    implicit def row2LongOption( row: ResultSetRow ) = LongColumnType( row ).nextValueOption
    implicit def row2FloatOption( row: ResultSetRow ) = FloatColumnType( row ).nextValueOption
    implicit def row2DoubleOption( row: ResultSetRow ) = DoubleColumnType( row ).nextValueOption
    implicit def row2BigDecimalOption( row: ResultSetRow ) = BigDecimalColumnType( row ).nextValueOption
    implicit def row2StringOption( row: ResultSetRow ) = StringColumnType( row ).nextValueOption
    implicit def row2DateOption( row: ResultSetRow ) = DateColumnType( row ).nextValueOption
    implicit def row2DateTimeOption( row: ResultSetRow ) = DateTimeColumnType( row ).nextValueOption
    implicit def row2LocalDateOption( row: ResultSetRow ) = LocalDateColumnType( row ).nextValueOption
    implicit def row2LocalTimeOption( row: ResultSetRow ) = LocalTimeColumnType( row ).nextValueOption
    implicit def row2DurationOption( row: ResultSetRow ) = DurationColumnType( row ).nextValueOption
}

object ResultSetRowColumnImplicits {
    implicit def row2Boolean( column: Column )(implicit row: ResultSetRow) = BooleanColumnType( row ).columnValue(column.name)
    implicit def row2Int( column: Column )(implicit row: ResultSetRow): Int = IntColumnType( row ).columnValue(column.name)
    implicit def row2Long( column: Column )(implicit row: ResultSetRow): Long = LongColumnType( row ).columnValue(column.name)
    implicit def row2Float( column: Column )(implicit row: ResultSetRow) = FloatColumnType( row ).columnValue(column.name)
    implicit def row2Double( column: Column )(implicit row: ResultSetRow) = DoubleColumnType( row ).columnValue(column.name)
    implicit def row2BigDecimal( column: Column )(implicit row: ResultSetRow) = BigDecimalColumnType( row ).columnValue(column.name)
    implicit def row2String( column: Column )(implicit row: ResultSetRow) = StringColumnType( row ).columnValue(column.name)
    implicit def row2Date( column: Column )(implicit row: ResultSetRow) = DateColumnType( row ).columnValue(column.name)
    implicit def row2DateTime( column: Column )(implicit row: ResultSetRow) = DateTimeColumnType( row ).columnValue(column.name)
    implicit def row2LocalDate( column: Column )(implicit row: ResultSetRow) = LocalDateColumnType( row ).columnValue(column.name)
    implicit def row2LocalTime( column: Column )(implicit row: ResultSetRow) = LocalTimeColumnType( row ).columnValue(column.name)
    implicit def row2LocalDateTime( column: Column )(implicit row: ResultSetRow) = LocalDateTimeColumnType( row ).columnValue(column.name)
    implicit def row2Duration( column: Column )(implicit row: ResultSetRow) = DurationColumnType( row ).columnValue(column.name)

    implicit def row2BooleanOption( column: Column )(implicit row: ResultSetRow) = BooleanColumnType( row ).columnValueOption(column.name)
    implicit def row2IntOption( column: Column )(implicit row: ResultSetRow) = IntColumnType( row ).columnValueOption(column.name)
    implicit def row2LongOption( column: Column )(implicit row: ResultSetRow) = LongColumnType( row ).columnValueOption(column.name)
    implicit def row2FloatOption( column: Column )(implicit row: ResultSetRow) = FloatColumnType( row ).columnValueOption(column.name)
    implicit def row2DoubleOption( column: Column )(implicit row: ResultSetRow) = DoubleColumnType( row ).columnValueOption(column.name)
    implicit def row2BigDecimalOption( column: Column )(implicit row: ResultSetRow) = BigDecimalColumnType( row ).columnValueOption(column.name)
    implicit def row2StringOption( column: Column )(implicit row: ResultSetRow) = StringColumnType( row ).columnValueOption(column.name)
    implicit def row2DateOption( column: Column )(implicit row: ResultSetRow) = DateColumnType( row ).columnValueOption(column.name)
    implicit def row2DateTimeOption( column: Column )(implicit row: ResultSetRow) = DateTimeColumnType( row ).columnValueOption(column.name)
    implicit def row2LocalDateOption( column: Column )(implicit row: ResultSetRow) = LocalDateColumnType( row ).columnValueOption(column.name)
    implicit def row2LocalTimeOption( column: Column )(implicit row: ResultSetRow) = LocalTimeColumnType( row ).columnValueOption(column.name)
    implicit def row2DurationOption( column: Column )(implicit row: ResultSetRow) = DurationColumnType( row ).columnValueOption(column.name)
}
