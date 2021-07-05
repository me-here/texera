package edu.uci.ics.texera.unittest.web.resource

import edu.uci.ics.texera.web.model.jooq.generated.Tables.WORKFLOW
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.tools.jdbc.{MockDataProvider, MockExecuteContext, MockResult}
import org.jooq.types.UInteger

import java.sql.{SQLException, Timestamp}

class MockWorkflowDataProvider extends MockDataProvider {
  @throws[SQLException]
  override def execute(ctx: MockExecuteContext): Array[MockResult] = {
    // You might need a DSLContext to create org.jooq.Result and org.jooq.Record objects
    val create = DSL.using(SQLDialect.MYSQL)
    val mock = new Array[MockResult](1)
    // The execute context contains SQL string(s), bind values, and other meta-data
    val sql = ctx.sql
    // Exceptions are propagated through the JDBC and jOOQ APIs
    if (sql.toUpperCase.startsWith("DROP"))
      throw new SQLException("Statement not supported: " + sql)
    else { // You decide, whether any given statement returns results, and how many
      if (sql.toUpperCase.startsWith("SELECT")) { // Always return one record
        val result = create.newResult(
          WORKFLOW.WID,
          WORKFLOW.CONTENT,
          WORKFLOW.NAME,
          WORKFLOW.CREATION_TIME,
          WORKFLOW.LAST_MODIFIED_TIME
        )
        result.add(
          create
            .newRecord(
              WORKFLOW.WID,
              WORKFLOW.CONTENT,
              WORKFLOW.NAME,
              WORKFLOW.CREATION_TIME,
              WORKFLOW.LAST_MODIFIED_TIME
            )
            .values(
              1.asInstanceOf[UInteger],
              "workflow-content",
              "test-workflow",
              new Timestamp(10000),
              new Timestamp(10000)
            )
        )
        mock(0) = new MockResult(1, result)
      } else {
        throw new SQLException("Statement not supported: " + sql)
      }
    }

    mock
  }
}
