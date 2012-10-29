package net.noerd.prequel

import org.joda.time.Duration

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach

class FormattablesSpec extends FunSpec with ShouldMatchers {

    val formatter = SQLFormatter.HSQLDBSQLFormatter    
    val formattables = List(
        ( "StringFormattable should escape foo", 
            StringFormattable( "foo"), "'foo'" 
        ),
        ( "LongFormattable should escape 1234", 
            LongFormattable( 1234L ), "1234" 
        ),
        ( "BooleanFormattable should escape true", 
            BooleanFormattable( true ), "true" 
        ),
        ( "BooleanFormattable should escape false", 
            BooleanFormattable( false ), "false" 
        ),
        ( "FloatFormattable should escape 1.500000", 
            FloatFormattable( 1.500000F ), "1.500000" 
        ),        
        ( "DoubleFormattable should escape 1.500000", 
            DoubleFormattable( 1.500000F ), "1.500000" 
        ),
        ( "DateTimeFormattable should escape 2010-03-13 13:00:00.0000",
          DateTimeFormattable( formatter.timeStampFormatter.parseDateTime( "2010-03-13 13:00:00.0000" ) ),
          "'2010-03-13 13:00:00.0000'"
        ),
        ( "LocalTimeFormattable should escape 13:00:00",
          TimeFormattable( formatter.timeFormatter.parseLocalTime( "13:00:00" ) ),
          "'13:00:00'"
        ),
        ( "LocalDateFormattable should escape 2010-03-13",
          LocalDateFormattable( formatter.dateFormatter.parseLocalDate( "2010-03-13" ) ),
          "'2010-03-13'"
        ),
        ( "LocalDateTimeFormattable should escape 2010-03-13 13:00:00.0000",
          LocalDateTimeFormattable( formatter.timeStampFormatter.parseLocalDateTime( "2010-03-13 13:00:00.0000" ) ),
          "'2010-03-13 13:00:00.0000'"
        ),
        ( "DurationFormattable should escape an Duration object",
            DurationFormattable( Duration.standardHours( 2 ) ), "7200000"
        ),
        ( "NullComparable should escape defined formattable", 
            NullComparable( Some( StringFormattable( "foo" ) ) ), "='foo'"
        ),
        ( "NullComparable should escape undefined formattable", 
            NullComparable( None ), "is null"
        ),
        ( "Nullable should escape defined formattables", 
            Nullable( Some( StringFormattable( "foo" ) ) ), "'foo'" 
        ),        
        ( "Nullable should escape undefined formattables", 
            Nullable( None ), "null" 
        ),
        ( "Identifier should leave string as they are", 
            Identifier( "this.is.a.stri'ng" ), "this.is.a.stri'ng"
        )
    )

    describe( "Formattables" ) {
        
        formattables.foreach { case( description, formattable, expected ) =>
            it( description ) {
                formattable.escaped( formatter ) should equal ( expected )
            }            
        }
    }
}