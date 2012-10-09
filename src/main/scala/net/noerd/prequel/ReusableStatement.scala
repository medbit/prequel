package net.noerd.prequel

import java.sql._

import org.joda.time.{LocalDate, DateTime}

/**
 * Wrapper around PreparedStatement making is easier to add parameters.
 *
 * The ReusableStatement can be used in two ways.
 *
 * ## Add parameters and then execute as a chain
 * statement << param1 << param2 << param3 <<!
 *
 * ## Set parameters and execute in on shot
 * statement.executeWith( param1, param2, param3 )
 */
class ReusableStatement(val wrapped: PreparedStatement, formatter: SQLFormatter) {
  private val StartIndex = 1
  private var parameterIndex = StartIndex

  /**
   * Adds the param to the query and returns this so that it
   * possible to chain several calls together
   * @return self to allow for chaining calls
   */
  def <<(param: Formattable): ReusableStatement = {
    param.addTo(this)
    this
  }

  /**
   * Alias of execute() included to look good with the <<
   * @return the number of affected records
   */
  def <<! = execute

  /**
   * Executes the statement with the previously set parameters
   * @return the number of affected records
   */
  def execute: Int = {
    parameterIndex = StartIndex
    wrapped.executeUpdate()
  }

  /**
   * Sets all parameters and executes the statement
   * @return the number of affected records
   */
  def executeWith(params: Formattable*): Int = {
    params.foreach(this << _)
    execute
  }

  /**
   * Executes the statement with the previously set parameters
   * @return the number of affected records
   */
  def select: ResultSet = {
    parameterIndex = StartIndex
    wrapped.executeQuery()
  }

  /**
   * Sets all parameters and executes the statement
   * @return the number of affected records
   */
  def selectWith(params: Formattable*): ResultSet = {
    params.foreach(this << _)
    select
  }

  /**
   * Add a String to the current parameter index
   */
  def addString(value: String) {
    addValue(() =>
      wrapped.setString(parameterIndex, formatter.escapeString(value))
    )
  }


  /**
   * Add an sql.Date to the current parameter index.
   */
  def addSqlDate(value: java.sql.Date) {
    addValue(() =>
      wrapped.setDate(parameterIndex, value)
    )
  }

  /**
   * Add a Date to the current parameter index. This is done by setTimestamp which
   * loses the Timezone information of the DateTime
   */
  def addDateTime(value: DateTime) {
    addValue(() =>
      wrapped.setTimestamp(parameterIndex, new Timestamp(value.getMillis))
    )
  }

  /**
   * Add a LocalDate to the current parameter index. This is done by setDate which
   * converts to an sql.Date
   */
  def addLocalDate(value: LocalDate) {
    addSqlDate(new Date(value.toDateTimeAtStartOfDay.getMillis))

  }

  /**
   * Add a Boolean to the current parameter index
   */
  def addBoolean(value: Boolean) {
    addValue(() => wrapped.setBoolean(parameterIndex, value))
  }

  /**
   * Add a Long to the current parameter index
   */
  def addLong(value: Long) {
    addValue(() => wrapped.setLong(parameterIndex, value))
  }

  /**
   * Add a Int to the current parameter index
   */
  def addInt(value: Int) {
    addValue(() => wrapped.setInt(parameterIndex, value))
  }

  /**
   * Add a Float to the current parameter index
   */
  def addFloat(value: Float) {
    addValue(() => wrapped.setFloat(parameterIndex, value))
  }

  /**
   * Add a Double to the current parameter index
   */
  def addDouble(value: Double) {
    addValue(() => wrapped.setDouble(parameterIndex, value))
  }

  /**
   * Add a BigDecimal to the current parameter index
   */
  def addBigDecimal(value: BigDecimal) {
    addValue(() => wrapped.setBigDecimal(parameterIndex, value.underlying()))
  }

  /**
   * Add Null to the current parameter index
   */
  def addNull() {
    addValue(() => wrapped.setNull(parameterIndex, Types.NULL))
  }

  private def addValue(f: () => Unit) {
    f.apply()
    parameterIndex = parameterIndex + 1
  }

  private[prequel] def close() {
    wrapped.close()
  }
}