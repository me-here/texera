package edu.uci.ics.texera.unittest.web.resource
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.Workflow
import edu.uci.ics.texera.web.resource.dashboard.WorkflowResource
import io.dropwizard.testing.junit5.{DropwizardExtensionsSupport, ResourceExtension}
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory
import org.jooq.impl.DSL
import org.jooq.tools.jdbc.MockConnection
import org.jooq.{DSLContext, SQLDialect}
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.{After, Before}
import org.junit.jupiter.api.Test
import org.scalatestplus.junit.AssertionsForJUnit

@ExtendWith(Array(classOf[DropwizardExtensionsSupport]))
class WorkflowResourceSpec extends AssertionsForJUnit {

  val provider = new MockWorkflowDataProvider()
  val connection = new MockConnection(provider)

  // Pass the mock connection to a jOOQ DSLContext:
  val context: DSLContext = DSL.using(connection, SQLDialect.MYSQL)

  private val workflowResource = ResourceExtension.builder
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
    .addResource(
      new WorkflowResource(context)
    )
    .build

  @Before def initialize() {}

  @After def tearDown(): Unit = {}

  @Test def verifyEasy() { // Uses ScalaTest assertions
    workflowResource.target("/workflow/1").request().get(classOf[Workflow])
  }
}
