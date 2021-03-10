package edu.uci.ics.texera.workflow.operators.sink.file;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

public enum ResultFileType {
    EXCEL("excel", ".csv");

    final String name;

    final String fileSuffix;

    ResultFileType(String name, String fileSuffix) {
        this.name = name;
        this.fileSuffix = fileSuffix;
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return StringUtils.isEmpty(fileSuffix) ? name : String.format("%s(%s)", name, fileSuffix);
    }
}
