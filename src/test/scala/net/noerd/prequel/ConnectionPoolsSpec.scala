package net.noerd.prequel

import java.sql.SQLException

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach

class ConnectionPoolsSpec extends FunSpec with ShouldMatchers with BeforeAndAfterEach {
    
    val config1 = DatabaseConfig( 
        driver = "org.hsqldb.jdbc.JDBCDriver",
        jdbcURL = "jdbc:hsqldb:mem:poolconfig1"
    )
    val config1Copy = DatabaseConfig( 
        driver = "org.hsqldb.jdbc.JDBCDriver",
        jdbcURL = "jdbc:hsqldb:mem:poolconfig1"
    )
    val config2 = DatabaseConfig( 
        driver = "org.hsqldb.jdbc.JDBCDriver",
        jdbcURL = "jdbc:hsqldb:mem:poolconfig2"
    )

    override def beforeEach() {
      ConnectionPools.reset()
    }
    override def afterEach() {
      ConnectionPools.reset()
    }
    describe( "ConnectionPools" ) {
        
        describe( "getOrCreatePool" ) {
        
            it( "should create a new pool for each unique Configuration" ) {
            
                ConnectionPools.getOrCreatePool( config1 )
                ConnectionPools.nbrOfPools should be (1)

                ConnectionPools.getOrCreatePool( config2 )
                ConnectionPools.nbrOfPools should be (2)
            }
            
            it( "should reuse an existing pool if the configuration is the same" ) {
                
                ConnectionPools.getOrCreatePool( config1 )
                ConnectionPools.nbrOfPools should be (1)

                ConnectionPools.getOrCreatePool( config1 )
                ConnectionPools.nbrOfPools should be (1)

                ConnectionPools.getOrCreatePool( config1Copy )
                ConnectionPools.nbrOfPools should be (1)
            }
        }
    }

}