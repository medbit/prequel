package net.noerd.prequel

import org.joda.time.{LocalDate, DateTime, Duration}

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach

import net.noerd.prequel.SQLFormatterImplicits._

trait ColumnTypeSpec[ T ] extends FunSpec with ShouldMatchers {
    
    val database = TestDatabase.config
    val testIdentifier = columnTypeFactory.getClass.getSimpleName.replace( "$", "" )
    
    def sqlType: String
    def testValue: Formattable
    def columnTypeFactory: ColumnTypeFactory[T]
    
    describe( testIdentifier ) {
        
        it( "should handle both defined and undefined value" ) { database.transaction { tx =>

            // Using the test identifier and sql type as table since tables
            // in hsqldb are not transaction safe objects.
            val table = testIdentifier + sqlType.replaceAll("[^a-zA-Z]", "")

            tx.execute( 
                "create table ?(c1 ?, c2 ?)", 
                Identifier( table ), Identifier( sqlType ), Identifier( sqlType )
            )
            tx.execute( "insert into ? values(?, null)", Identifier( table ), testValue )

            tx.select( "select c1, c2 from ?", Identifier( table ) ) { row =>
                columnTypeFactory(row ).nextValue should equal (testValue.value)
                columnTypeFactory(row ).nextValueOption should be (None)
            }
            tx.rollback()
        } }
    }
}

class BigDecimalColumnTypeSpec extends ColumnTypeSpec[ BigDecimal ] {
  def sqlType = "numeric(8,2)"
  val testValue = BigDecimalFormattable( new BigDecimal(new java.math.BigDecimal(42)) )
  def columnTypeFactory = BigDecimalColumnType
}

class DateTimeColumnTypeSpec extends ColumnTypeSpec[ DateTime ] {    
    def sqlType = "timestamp"
    val testValue = DateTimeFormattable( new DateTime )
    def columnTypeFactory = DateTimeColumnType
}

class LocalDateColumnTypeSpec extends ColumnTypeSpec[ DateTime] {
  def sqlType = "date"
  val testValue = DateTimeFormattable( new LocalDate )
  def columnTypeFactory = DateTimeColumnType
}

class DurationColumnTypeSpec extends ColumnTypeSpec[ Duration ] {
    def sqlType = "bigint"
    val testValue = DurationFormattable( Duration.standardHours( 24 ) )
    def columnTypeFactory = DurationColumnType
}

class NStringColumnTypeSpec extends ColumnTypeSpec[ String ] {
    def sqlType = "nvarchar(256)" // note: nvarchar is synonym for varchar in hsqldb
    val testValue = NStringFormattable( "hello this is a string in n-charset; some unicode: åäö 델리만쥬" )
    def columnTypeFactory = NStringColumnType
}

class StringColumnTypeSpec extends ColumnTypeSpec[ String ] {
    def sqlType = "varchar(256)"
    val testValue = StringFormattable( "hello this is a simple string" )
    def columnTypeFactory = StringColumnType
}

class IntColumnTypeSpec extends ColumnTypeSpec[ Int ] {    
    def sqlType = "int"
    val testValue = IntFormattable( 42 )
    def columnTypeFactory = IntColumnType
}

class LongColumnTypeSpec extends ColumnTypeSpec[ Long ] {    
    def sqlType = "bigint"
    val testValue = LongFormattable( 498902382837L )
    def columnTypeFactory = LongColumnType
}

class BooleanColumnTypeSpec extends ColumnTypeSpec[ Boolean ] {    
    def sqlType = "boolean"
    val testValue = BooleanFormattable( true )
    def columnTypeFactory = BooleanColumnType
}

class DoubleColumnTypeSpec extends ColumnTypeSpec[ Double ] {
    def sqlType = "real"
    val testValue = DoubleFormattable( 2454354.2737 )
    def columnTypeFactory = DoubleColumnType
}

class FloatColumnTypeSpec extends ColumnTypeSpec[ Float ] {
    def sqlType = "real"
    val testValue = FloatFormattable( 2.42f )
    def columnTypeFactory = FloatColumnType
}