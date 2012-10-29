package net.noerd.prequel

import java.util.Date

import org.apache.commons.lang.StringEscapeUtils.escapeSql

import org.joda.time._
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat
import scala.Some

/**
 * Currently a private class responsible for formatting SQL used in 
 * transactions (@see Transaction). It does properly format standards 
 * classes like DateTime, Floats, Longs and Integers as well as some 
 * SQL specific classes like Nullable, NullComparable and Identifier. 
 * See their documentation for more info on how to use them.
 */
class SQLFormatter(
    val timeStampFormatter: DateTimeFormatter,
    val dateFormatter: DateTimeFormatter,
    val timeFormatter: DateTimeFormatter
) {
    private val sqlQuote = "'"

    def format( sql: String, params: Formattable* ): String = formatSeq( sql, params.toSeq )

    def formatSeq( sql: String, params: Seq[ Formattable ] ): String = {
        sql.replace("?", "%s").format( params.map( p => p.escaped( this ) ): _* )
    }

    /**
     * Escapes  "'" and "\" in the string for use in a sql query
     */
    def escapeString( str: String ): String = escapeSql( str ).replace( "\\", "\\\\" )

    /**
     * Quotes the passed string according to the formatter
     */
    def quoteString( str: String ): String = {
        val sb = new StringBuilder
        sb.append( sqlQuote ).append( str ).append( sqlQuote )
        sb.toString
    }

    /**
     * Escapes and quotes the given string
     */
    def toSQLString( str: String ): String = quoteString( escapeString( str ) )
}

object SQLFormatter {
    /**
     * SQLFormatter for dbs supporting ISODateTimeFormat
     */
    val DefaultSQLFormatter = SQLFormatter()
    /**
     * SQLFormatter for usage with HSQLDB.
     */
    val HSQLDBSQLFormatter = SQLFormatter(
        timeStampFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSS"),
        dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd"),
        timeFormatter = DateTimeFormat.forPattern("HH:mm:ss")
    )

    private[ prequel ] def apply(
        timeStampFormatter: DateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis,
        dateFormatter: DateTimeFormatter = ISODateTimeFormat.date,
        timeFormatter: DateTimeFormatter = ISODateTimeFormat.timeNoMillis
    ) = {
        new SQLFormatter( timeStampFormatter , dateFormatter, timeFormatter )
    }
}

object SQLFormatterImplicits {

    implicit def string2Formattable( wrapped: String ) = StringFormattable( wrapped )
    def nstring2Formattable( wrapped: String ) = NStringFormattable( wrapped ) // don't clash with the above implicit
    implicit def boolean2Formattable( wrapped: Boolean ) = BooleanFormattable( wrapped )
    implicit def long2Formattable( wrapped: Long ) = LongFormattable( wrapped )
    implicit def int2Formattable( wrapped: Int ) = IntFormattable( wrapped )
    implicit def float2Formattable( wrapped: Float ) = FloatFormattable( wrapped )
    implicit def double2Formattable( wrapped: Double ) = DoubleFormattable( wrapped )
    implicit def bigDecimal2Formattable( wrapped: BigDecimal ) = BigDecimalFormattable( wrapped )
    implicit def dateTime2Formattable( wrapped: DateTime ) = DateTimeFormattable( wrapped )
    implicit def date2Formattable( wrapped: Date ) = DateTimeFormattable( wrapped )
    implicit def localDate2Formattable( wrapped: LocalDate ) = DateTimeFormattable( wrapped )
    implicit def localDateTime2Formattable( wrapped: LocalDateTime ) = LocalDateTimeFormattable( wrapped )
    implicit def localTime2Formattable( wrapped: LocalTime ) = TimeFormattable( wrapped )
    implicit def duration2Formattable( wrapped: Duration ) = new DurationFormattable( wrapped )

    implicit def stringOpt2Formattable( wrapped: Option[String] ):Formattable = wrapped match {
      case Some( wrapperSome ) => StringFormattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def booleanOpt2Formattable( wrapped: Option[Boolean] ):Formattable = wrapped match {
      case Some( wrapperSome ) => BooleanFormattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def longOpt2Formattable( wrapped: Option[Long] ):Formattable = wrapped match {
      case Some( wrapperSome ) =>  LongFormattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def intOpt2Formattable( wrapped: Option[Int] ):Formattable = wrapped match {
      case Some( wrapperSome ) => int2Formattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def floatOpt2Formattable( wrapped: Option[Float] ):Formattable = wrapped match {
      case Some( wrapperSome ) => float2Formattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def doubleOpt2Formattable( wrapped: Option[Double] ):Formattable = wrapped match {
      case Some( wrapperSome ) => double2Formattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def bigDecimalOpt2Formattable( wrapped: Option[BigDecimal] ):Formattable = wrapped match {
      case Some( wrapperSome ) => bigDecimal2Formattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def dateTimeOpt2Formattable( wrapped: Option[DateTime] ):Formattable = wrapped match {
      case Some( wrapperSome ) => dateTime2Formattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def dateOpt2Formattable( wrapped: Option[Date] ):Formattable = wrapped match {
      case Some( wrapperSome ) =>  date2Formattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def localDateOpt2Formattable( wrapped: Option[LocalDate] ):Formattable = wrapped match {
      case Some( wrapperSome ) =>  localDate2Formattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def localDateTimeOpt2Formattable( wrapped: Option[LocalDateTime] ):Formattable = wrapped match {
      case Some( wrapperSome ) =>  localDateTime2Formattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def localTimeOpt2Formattable( wrapped: Option[LocalTime] ):Formattable = wrapped match {
      case Some( wrapperSome ) =>  localTime2Formattable( wrapperSome )
      case None => Nullable(None)
    }

    implicit def durationOpt2Formattable( wrapped: Option[Duration] ):Formattable = wrapped match {
      case Some( wrapperSome ) => duration2Formattable( wrapperSome )
      case None => Nullable(None)
    }

}