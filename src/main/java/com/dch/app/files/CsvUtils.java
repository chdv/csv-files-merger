package com.dch.app.files;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dcherdyntsev on 16.07.2015.
 */
public class CsvUtils {

    public static final String FILES_HOME = "C:\\Workspace\\!files\\";
    public static final char DEFAULT_COLUMNS_DELIMETER = ';';
    public static final char QUOTE = '"';

    public static String truncateQuotes(String str) {
        String s = str.trim();
        if(s.length() == 0)
            return s;
        if(s.charAt(0) == QUOTE && s.charAt(str.length() - 1) == QUOTE) {
            s = s.substring(1, str.length() - 1);
        }
        return s;
    }

    public static int getIndxByName(String[] columns, String name) {
        int index = 0;
        for(String c : columns) {
            if(name.equals(c)) {
                return index;
            }
            index++;
        }
        throw new IllegalArgumentException("column " + name + " not found");
    }

    public static String[] split(String line, char delimeter) {
        char quot = QUOTE;
        char sys = '\\';
        List<String> result = new ArrayList<>();
        boolean quotSection = false;
        boolean sysSection = false;
        boolean useSys = false;
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i<line.length(); i++) {
            char currentChar = line.charAt(i);
            boolean append = true;
            if(useSys && sysSection) {
                useSys = false;
                sysSection = false;
            }
            if(sysSection) {
                useSys = true;
            }
            if(currentChar==sys) {
                sysSection = true;
            } if(currentChar==quot && !sysSection) {
                quotSection = !quotSection;
            } else if(currentChar==delimeter && !quotSection && !sysSection) {
                result.add(builder.toString());
                builder = new StringBuilder();
                append = false;
            }
            if(append)
                builder.append(currentChar);
        }
        result.add(builder.toString());
        return result.toArray(new String[]{});
    }

    public static String trim2null(String str) {
        if(str.trim().length() == 0)
            return null;
        return str;
    }

    public static long getBucketIndex(long count, long key) {
        if(key == -1L)
            return key;
        return (count - 1) & hash(key);
    }

    private static final int hash(long key) {
        int h;
        return (h = Long.hashCode(key)) ^ (h >>> 16);
    }

    public static void writeln(Writer wr, String line) throws IOException {
        wr.write(line + "\n");
    }

    public static String[] truncateQuotes(String[] row) {
        String[] result = new String[row.length];
        int index = 0;
        for(String c : row) {
            result[index] = truncateQuotes(row[index]);
            index++;
        }
        return result;
    }

    public static String getFilePath(String fileName) {
        return FILES_HOME + fileName;
    }

    public static String toString(Object[] obj) {
        StringBuilder build = new StringBuilder();
        int indx = 0;
        for(Object o : obj) {
            build.append("\n" + indx + "   " + o);
            indx++;
        }
        return build.toString();
    }

    public static boolean isNumeric(String str) {
        try {
            Long.parseLong(str);
            return true;
        } catch(Exception e) {
            return false;
        }
    }

}
