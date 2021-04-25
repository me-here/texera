package edu.uci.ics.texera.workflow.operators.sink.file;

import edu.uci.ics.texera.web.WebUtils;
import edu.uci.ics.texera.workflow.common.Utils;

import java.io.File;
import java.nio.file.Path;

public class FileSinkOpHelper {

    private static final Path resultBaseDirectory =
            Utils.amberHomePath()
                    .resolve("../user-resources")
            .resolve(WebUtils.config().getString("userResource.workflowResultPath"));

    public static File locateResultFile(String uid, String fileName) {
        return resultBaseDirectory.resolve(uid).resolve(fileName).toFile();
    }
}
