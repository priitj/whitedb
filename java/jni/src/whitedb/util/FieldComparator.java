package wgandalf.util;

import java.util.Comparator;
import java.lang.reflect.Field;

public class FieldComparator implements Comparator<Field> {
    public int compare(Field field1, Field field2) {
        return field1.getName().compareTo(field2.getName());
    }
}
