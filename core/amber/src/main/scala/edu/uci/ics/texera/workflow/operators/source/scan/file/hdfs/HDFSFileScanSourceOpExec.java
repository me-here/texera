package edu.uci.ics.texera.workflow.operators.source.scan.file.hdfs;

import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.operators.source.scan.file.FileScanSourceOpExec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSFileScanSourceOpExec extends FileScanSourceOpExec {


    private final String path;
    private final String hdfsIP;

    HDFSFileScanSourceOpExec(String hdfsIP, String path, Schema schema, char delimiter,
                        boolean hasHeader, long startOffset, long endOffset){
        super(schema,delimiter,hasHeader,startOffset,endOffset);
        this.path = path;
        this.hdfsIP = hdfsIP;

    }

    @Override
    public InputStream getPositionedInputStream() throws IOException {
        try {
            FileSystem fs = FileSystem.get(new URI(hdfsIP), new Configuration());
            FSDataInputStream inputStream = fs.open(new Path(path));
            inputStream.seek(startOffset);
            return inputStream;
        } catch (URISyntaxException e) {
            throw new IOException("cannot read from hdfs");
        }
    }
}
