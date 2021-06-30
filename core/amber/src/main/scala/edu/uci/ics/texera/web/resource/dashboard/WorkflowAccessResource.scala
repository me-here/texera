package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  WORKFLOW,
  WORKFLOW_OF_USER,
  WORKFLOW_USER_ACCESS
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  WorkflowDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{
  Workflow,
  WorkflowOfUser,
  WorkflowUserAccess
}
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.web.resource.dashboard
import io.dropwizard.jersey.sessions.Session
import org.jooq.types.UInteger

import java.util
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.immutable.HashMap

object Access extends Enumeration {
  type Access = Value
  val Read: dashboard.Access.Value = Value("read")
  val Write: dashboard.Access.Value = Value("write")
  val None: dashboard.Access.Value = Value("none")
  val NoRecord: dashboard.Access.Value = Value("No Record")
}

class AccessResponse(uid: UInteger, wid: UInteger, level: String)

object WorkflowAccessResource {
  def checkAccessLevel(wid: UInteger, uid: UInteger): Access.Value = {
    val accessDetail = SqlServer.createDSLContext
      .select(WORKFLOW_USER_ACCESS.READ_PRIVILEGE, WORKFLOW_USER_ACCESS.WRITE_PRIVILEGE)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.eq(uid)))
      .fetch()
    if (accessDetail.isEmpty) return Access.NoRecord
    if (accessDetail.getValue(0, 1) == true) {
      Access.Write
    } else if (accessDetail.getValue(0, 0) == true) {
      Access.Read
    } else {
      Access.None
    }
  }

  def hasReadAccess(wid: UInteger, uid: UInteger): Boolean = {
    WorkflowAccessResource.checkAccessLevel(wid, uid).eq(Access.Read)
  }

  def hasWriteAccess(wid: UInteger, uid: UInteger): Boolean = {
    WorkflowAccessResource.checkAccessLevel(wid, uid).eq(Access.Write)
  }
}

/*This class handles all requests and updates related to the contents about different levels of user access to the workflows*/
@Path("/workflowaccess")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowAccessResource {
  final private val workflowDao = new WorkflowDao(SqlServer.createDSLContext.configuration)
  final private val workflowOfUserDao = new WorkflowOfUserDao(
    SqlServer.createDSLContext.configuration
  )
  final private val workflowUserAccessDao = new WorkflowUserAccessDao(
    SqlServer.createDSLContext().configuration()
  )

  /**
    * This method identifies the user access level of the given workflow
    *
    * @param wid     the given workflow
    * @param session the session indicating current User
    * @return "None" or "Read" or "Write" or "Read& Write"
    */
  @GET
  @Path("/workflow/{wid}/level")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveUserAccessLevel(
      @PathParam("wid") wid: UInteger,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val uid = user.getUid
        val accessLevel = WorkflowAccessResource.checkAccessLevel(wid, uid)
        Response.ok(HashMap("UID" -> uid, "WID" -> wid, "Level" -> accessLevel)).build()
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

}
