package edu.uci.ics.texera.workflow.operators.source.scan.file;

import com.google.common.base.Verify;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.scanner.BufferedBlockReader;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.Iterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class FileScanSourceOpExec implements SourceOperatorExecutor {

    private final char delimiter;
    private BufferedBlockReader reader = null;
    protected final long startOffset;
    private final long endOffset;
    private final Schema schema;
    private final boolean hasHeader;

    public FileScanSourceOpExec(Schema schema, char delimiter,
                        boolean hasHeader, long startOffset, long endOffset) {
        this.delimiter = delimiter;
        this.schema = schema;
        this.hasHeader = hasHeader;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }


    public abstract InputStream getPositionedInputStream() throws IOException;

    @Override
    public Iterator<Tuple> produceTexeraTuple() {
        return new Iterator<Tuple>() {

            @Override
            public boolean hasNext() {
                try {
                    return reader.hasNext();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Tuple next() {
                try {

                    // obtain String representation of each field
                    // a null value will present if omit in between fields, e.g., ['hello', null, 'world']
                    String[] fields = reader.readLine();
                    if (fields == null || Arrays.stream(fields).noneMatch(Objects::nonNull)) {
                        // discard tuple if it's null or it only contains null
                        // which means it will always discard Tuple(null) from readLine()
                        return null;
                    }

                    Verify.verify(schema != null);

                    // however the null values won't present if omitted in the end, we need to match nulls.
                    if (fields.length != schema.getAttributes().size()) {
                        fields = Stream.concat(Arrays.stream(fields),
                                IntStream.range(0, schema.getAttributes().size() - fields.length)
                                        .mapToObj(i -> null)).toArray(String[]::new);
                    }

                    // parse Strings into inferred AttributeTypes

                    Object[] parsedFields = AttributeTypeUtils.parseFields(fields,
                            schema.getAttributes().stream().map(Attribute::getType).toArray(AttributeType[]::new)

                    );

                    return Tuple.newBuilder().add(schema, parsedFields).build();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        };
    }


    @Override
    public void open() {
        try {
            reader = new BufferedBlockReader(getPositionedInputStream(), endOffset - startOffset, delimiter, null);
            // skip line if this worker reads from middle of a file
            if (startOffset > 0)
                reader.readLine();
            // skip line if this worker reads the start of a file, and the file has a header line
            if (startOffset == 0 && hasHeader) {
                reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}
