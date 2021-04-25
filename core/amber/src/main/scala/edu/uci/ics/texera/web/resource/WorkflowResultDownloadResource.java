package edu.uci.ics.texera.web.resource;

import com.google.common.io.Files;
import edu.uci.ics.texera.web.WebUtils;
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User;
import edu.uci.ics.texera.web.resource.auth.UserResource;
import edu.uci.ics.texera.workflow.operators.sink.file.FileSinkOpHelper;
import edu.uci.ics.texera.workflow.operators.sink.file.ResultFileType;
import io.dropwizard.jersey.sessions.Session;
import scala.Option;

import javax.servlet.http.HttpSession;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

@Path("/download")
@Consumes(MediaType.APPLICATION_JSON)
public class WorkflowResultDownloadResource {

    @GET
    @Path("/result")
    public Response downloadFile(@Session HttpSession session,
                                 @QueryParam("fileName") String fileName,
                                 @QueryParam("downloadType") String downloadType) {

        Option<User> userOptional = UserResource.getUser(session);
        if (userOptional.isEmpty()) {
            return Response.status(Status.UNAUTHORIZED).build();
        }
        String userId = userOptional.get().getUid().toString();

        String downloadName = fileName + ResultFileType.getFileSuffix(downloadType);
        File file = FileSinkOpHelper.locateResultFile(userId, downloadName);

        if (!file.exists()) {
            return Response.status(Status.NOT_FOUND).build();
        }

        // sending a FileOutputStream/ByteArrayOutputStream directly will cause MessageBodyWriter not found issue for jersey
        // so we create our own stream.
        StreamingOutput fileStream = new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                byte[] data = Files.toByteArray(file);
                output.write(data);
                output.flush();
            }
        };

        return Response.ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition", String.format("attachment; filename=%s", downloadName))
                .build();
    }

}
