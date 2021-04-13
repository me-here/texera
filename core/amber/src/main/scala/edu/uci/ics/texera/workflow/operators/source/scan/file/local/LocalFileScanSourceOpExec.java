package edu.uci.ics.texera.workflow.operators.source.scan.file.local;

import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.operators.source.scan.file.FileScanSourceOpExec;
import org.tukaani.xz.SeekableFileInputStream;

import java.io.IOException;
import java.io.InputStream;

public class LocalFileScanSourceOpExec extends FileScanSourceOpExec {

    private final String path;

    LocalFileScanSourceOpExec(String path, Schema schema, char delimiter,
                              boolean hasHeader, long startOffset, long endOffset){
        super(schema,delimiter,hasHeader,startOffset,endOffset);
        this.path = path;
    }

    @Override
    public InputStream getPositionedInputStream() throws IOException {
        SeekableFileInputStream stream = new SeekableFileInputStream(path);
        stream.seek(startOffset);
        return stream;
    }
}
