package hadoop.mapreduce.chicago;

import java.util.ArrayList;
import java.util.List;

public final class CsvUtils {

    private CsvUtils() {
    }

    public static String[] parseLine(String line) {
        List<String> values = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int index = 0; index < line.length(); index++) {
            char character = line.charAt(index);

            if (character == '"') {
                if (inQuotes && index + 1 < line.length() && line.charAt(index + 1) == '"') {
                    current.append('"');
                    index++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (character == ',' && !inQuotes) {
                values.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(character);
            }
        }

        values.add(current.toString().trim());
        return values.toArray(new String[0]);
    }
}
