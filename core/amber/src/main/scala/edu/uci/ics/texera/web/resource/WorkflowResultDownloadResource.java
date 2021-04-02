package edu.uci.ics.texera.web.resource;

import com.google.common.io.Files;
import edu.uci.ics.texera.web.WebUtils;
import edu.uci.ics.texera.workflow.operators.sink.file.ResultFileType;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

@Path("/download")
@Consumes(MediaType.APPLICATION_JSON)
public class WorkflowResultDownloadResource {

    @GET
    @Path("/result")
    public Response downloadFile(@QueryParam("userId") String userId,
                                 @QueryParam("fileName") String fileName,
                                 @QueryParam("downloadType") String downloadType)
            throws IOException {
        java.nio.file.Path directory = WebUtils.resultBaseDirectory().resolve(userId);
        String downloadName = fileName + ResultFileType.getFileSuffix(downloadType);
        File file = directory.resolve(downloadName).toFile();


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
