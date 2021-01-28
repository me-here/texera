package edu.uci.ics.texera.web.resource.dashboard

import java.util

import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER_DICTIONARY
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDictionaryDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.UserDictionary
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.auth.UserResource

import io.dropwizard.jersey.sessions.Session

import org.jooq.types.UInteger

import play.api.libs.json
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import org.jooq.JSON

import com.google.api.Http

import scala.collection.JavaConversions._

/**
  * This class handles requests to read and write the user dictionary
  * It sends sql queries to the MysqlDB for rows of the user_dictionary table
  * The details of user_dictionary can be found in /core/scripts/sql/texera_ddl.sql
  */
@Path("/users/dictionary")
@Produces(Array(MediaType.TEXT_PLAIN))
class UserDictionaryResource {
  final private val userDictionaryDao = new UserDictionaryDao(SqlServer.createDSLContext.configuration)

  /**
    * This method either retrieves all values from the current in-session user's dictionary, or retrieves a value corresponding to the "key" query parameter
    */
  @GET
  def getValue(@Session session: HttpSession, @QueryParam("key") key: String): Response = {
    
    // check that user is logged in and authorized
    val user = UserResource.getUser(session)
    if (user == null) return Response.status(Response.Status.UNAUTHORIZED).build()

    if (key == null) {
      return getAllValues(user)
    } else {
      return getValueByKey(user, key)
    }
  }

  private def getValueByKey(user: User, key: String): Response = {

    val queryResult = SqlServer.createDSLContext
      .select()
      .from(USER_DICTIONARY) 
      .where(USER_DICTIONARY.UID.eq(user.getUid()))
      .and(USER_DICTIONARY.KEY.eq(key))
      .fetchInto(classOf[UserDictionary])
    
    // check that query doesn't return more than one result
    if (queryResult.size() > 1) return Response.serverError.entity("key corresponds to more than one entry").build()

    // check that there is 1 result
    if (queryResult.size() == 0) return Response.status(Response.Status.BAD_REQUEST).entity("no such entry").build()
    // else return the entry
    else return Response.ok(
      jsonElementToString(
        queryResult.get(0).getValue()
      )
    ).build()
  }

  private def getAllValues(user: User): Response = {
    val queryResult = SqlServer.createDSLContext
      .select()
      .from(USER_DICTIONARY) 
      .where(USER_DICTIONARY.UID.eq(user.getUid()))
      .fetchInto(classOf[UserDictionary])

    // check for zero values, return empty json object
    if (queryResult.size() == 0) return Response.ok("{}").build()

    var jObj = json.JsObject(
        (for(dictEntry <- queryResult) yield (
          dictEntry.getKey(), 
          json.Json.parse(dictEntry.getValue().toString()).as[json.JsArray].value(0))
        ).toSeq
    )
    
    return Response.ok(
      jObj.toString()
    ).build()
  }

  /**
    * This method saves a value into the current in-session user's dictionary
    */
  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  def setValue(@Session session: HttpSession, @QueryParam("key") key: String, value: String): Response = {
    // check that request payload isn't empty
    if (value.length() == 0) return Response.status(Response.Status.BAD_REQUEST).entity("empty payload").build()
    
    // check that user is logged in and authorized
    val user = UserResource.getUser(session)
    if (user == null) return Response.status(Response.Status.UNAUTHORIZED).build()

    val queryResult = SqlServer.createDSLContext
      .select()
      .from(USER_DICTIONARY) 
      .where(USER_DICTIONARY.UID.eq(user.getUid()))
      .and(USER_DICTIONARY.KEY.eq(key))
      .fetchInto(classOf[UserDictionary])

    // check that query returns 1 or 0 results (dictionary entries should be unique)
    if (queryResult.size() > 1) return Response.serverError.entity("key corresponds to more than one entry").build()
    
    if (queryResult.size() == 0) {
      // insert new entry if no entry exists in the dictionary
      userDictionaryDao.insert(
        new UserDictionary(
          user.getUid(),
          key,
          stringToJsonElement(value)
        )
      )
      return Response.ok().build();
    } else {
      // if queryResult.size() == 1
      // update entry if entry exists in the dictionary
      queryResult.get(0).setValue(stringToJsonElement(value))
      userDictionaryDao.update(queryResult.get(0))
      return Response.ok().build()
    }

  }

  /**
    * This method deletes a key-value pair from the current in-session user's dictionary
    */
  @DELETE
  def deleteValue(@Session session: HttpSession, @QueryParam("key") key: String): Response = {
    // check that user is logged in and authorized
    val user = UserResource.getUser(session)
    if (user == null) return Response.status(Response.Status.UNAUTHORIZED).build()

    val queryResult = SqlServer.createDSLContext
      .select()
      .from(USER_DICTIONARY) 
      .where(USER_DICTIONARY.UID.eq(user.getUid()))
      .and(USER_DICTIONARY.KEY.eq(key))
      .fetchInto(classOf[UserDictionary])

    // check that query doesn't return more than one result
    if (queryResult.size() > 1) return Response.serverError.entity("key corresponds to more than one entry").build()

    // check that there is 1 result
    if (queryResult.size() == 0) return Response.status(Response.Status.BAD_REQUEST).entity("no such entry").build()
    // else delete the entry
    else {
      userDictionaryDao.delete(queryResult.get(0))
      return Response.ok(
        jsonElementToString(
          queryResult.get(0).getValue()
        )
      ).build()
    }
  }

  private def isValidJsonElement(string: String): Boolean = {
    try {
      json.Json.parse(string)
      return true
    } catch {
      case e: JsonParseException => return false
      case e: JsonMappingException => return false
    }
  }

  // converts any string to json by inserting it into an array
  private def stringToJsonElement(string: String): JSON = {
    if (isValidJsonElement(string)) {
      return JSON.json(json.JsArray(Seq(json.Json.parse(string))).toString())
    } else {
      return JSON.json(json.JsArray(Seq(new json.JsString(string))).toString())
    }
  }

  private def jsonElementToString(_json: JSON): String = {
    val jArray = json.Json.parse(_json.toString()).as[json.JsArray]
    if (jArray.apply(0).validate[json.JsString].isSuccess) {
      var jString = jArray.apply(0).as[json.JsString].toString()
      return jString.substring(1, jString.length()-1) // trim off double quotes -> "..."
    } else {
      return jArray.apply(0).toString()
    }
  }

}