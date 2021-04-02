package edu.uci.ics.texera.workflow.operators.sink.file;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public enum ResultFileType {
    EXCEL("excel", ".csv");

    final String name;

    final String fileSuffix;

    static final String SUFFIX_DOES_NOT_EXIST = ".NOT_EXISTED";

    ResultFileType(String name, String fileSuffix) {
        this.name = name;
        this.fileSuffix = fileSuffix;
    }

    public static String getFileSuffix(String name){
        return Arrays.stream(values())
                .filter(target -> target.name.equals(name))
                .findAny()
                .map(type -> type.fileSuffix)
                .orElse(SUFFIX_DOES_NOT_EXIST);
    }

    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return StringUtils.isEmpty(fileSuffix) ? name : String.format("%s(%s)", name, fileSuffix);
    }
}
