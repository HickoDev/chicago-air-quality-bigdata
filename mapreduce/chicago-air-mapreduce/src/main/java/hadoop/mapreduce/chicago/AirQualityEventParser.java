package hadoop.mapreduce.chicago;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AirQualityEventParser {

    private static final List<DateTimeFormatter> TIMESTAMP_FORMATTERS = Arrays.asList(
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("MM/dd/yyyy hh:mm:ss a")
                    .toFormatter(Locale.US),
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("MM/dd/yyyy HH:mm:ss")
                    .toFormatter(Locale.US),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.US),
            DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.US),
            DateTimeFormatter.ISO_LOCAL_DATE
    );

    private AirQualityEventParser() {
    }

    public static Event parse(
            String line,
            int timestampIndex,
            int sensorIndex,
            int pm25Index,
            int no2Index) {
        String trimmed = line == null ? "" : line.trim();
        if (trimmed.isEmpty()) {
            return null;
        }

        if (trimmed.startsWith("{")) {
            return parseJsonEvent(trimmed);
        }

        return parseCsvEvent(trimmed, timestampIndex, sensorIndex, pm25Index, no2Index);
    }

    public static String extractDate(String timestampValue) {
        if (timestampValue == null) {
            return null;
        }

        String cleaned = timestampValue.trim();
        if (cleaned.isEmpty()) {
            return null;
        }

        for (DateTimeFormatter formatter : TIMESTAMP_FORMATTERS) {
            try {
                return LocalDateTime.parse(cleaned, formatter).toLocalDate().toString();
            } catch (DateTimeParseException ignored) {
                // Try parsing as a date-only value next.
            }

            try {
                return LocalDate.parse(cleaned, formatter).toString();
            } catch (DateTimeParseException ignored) {
                // Continue trying alternative timestamp formats.
            }
        }

        String datePart = cleaned.split("\\s+")[0];
        try {
            return LocalDate.parse(datePart, DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.US))
                    .toString();
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    public static Double parseDouble(String value) {
        if (value == null) {
            return null;
        }

        String cleaned = value.trim();
        if (cleaned.isEmpty()
                || cleaned.equalsIgnoreCase("null")
                || cleaned.equalsIgnoreCase("nan")) {
            return null;
        }

        try {
            return Double.parseDouble(cleaned);
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private static Event parseCsvEvent(
            String line,
            int timestampIndex,
            int sensorIndex,
            int pm25Index,
            int no2Index) {
        String[] columns = CsvUtils.parseLine(line);
        if (columns.length == 0 || looksLikeHeader(columns, timestampIndex, sensorIndex)) {
            return null;
        }

        int requiredIndex = Math.max(Math.max(timestampIndex, sensorIndex), Math.max(pm25Index, no2Index));
        if (columns.length <= requiredIndex) {
            return null;
        }

        return new Event(
                cleanString(columns[timestampIndex]),
                cleanString(columns[sensorIndex]),
                parseDouble(columns[pm25Index]),
                parseDouble(columns[no2Index])
        );
    }

    private static Event parseJsonEvent(String line) {
        String eventTime = firstJsonValue(line, "event_time", "timestamp", "time", "measurement_time");
        String sensorId = firstJsonValue(line, "sensor_id", "datasourceid", "node_id", "site_id", "device_id");
        Double pm25 = parseDouble(firstJsonValue(line, "pm25", "pm2_5", "pm2.5", "pm2_5_value"));
        Double no2 = parseDouble(firstJsonValue(line, "no2", "no2_value"));
        return new Event(eventTime, sensorId, pm25, no2);
    }

    private static String firstJsonValue(String json, String... fieldNames) {
        for (String fieldName : fieldNames) {
            String value = jsonValue(json, fieldName);
            if (value != null) {
                return cleanString(value);
            }
        }
        return null;
    }

    private static String jsonValue(String json, String fieldName) {
        String valuePattern = "\"((?:\\\\.|[^\"\\\\])*)\"|null|-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?";
        Pattern pattern = Pattern.compile("\"" + Pattern.quote(fieldName) + "\"\\s*:\\s*(" + valuePattern + ")");
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) {
            return null;
        }

        String rawValue = matcher.group(1);
        if (rawValue == null || rawValue.equals("null")) {
            return null;
        }

        if (rawValue.startsWith("\"") && rawValue.endsWith("\"")) {
            return unescapeJsonString(rawValue.substring(1, rawValue.length() - 1));
        }

        return rawValue;
    }

    private static String unescapeJsonString(String value) {
        return value
                .replace("\\\"", "\"")
                .replace("\\/", "/")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t")
                .replace("\\\\", "\\");
    }

    private static boolean looksLikeHeader(String[] columns, int timestampIndex, int sensorIndex) {
        if (columns.length <= Math.max(timestampIndex, sensorIndex)) {
            return false;
        }

        String sensorName = normalize(columns[sensorIndex]);
        String timestampName = normalize(columns[timestampIndex]);
        return sensorName.equals("datasourceid")
                || sensorName.equals("sensorid")
                || sensorName.equals("sensor_id")
                || timestampName.equals("time")
                || timestampName.equals("timestamp")
                || timestampName.equals("measurementtime");
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "");
    }

    private static String cleanString(String value) {
        String cleaned = value == null ? "" : value.trim();
        if (cleaned.isEmpty() || cleaned.equalsIgnoreCase("null")) {
            return null;
        }
        return cleaned;
    }

    public static final class Event {
        private final String eventTime;
        private final String sensorId;
        private final Double pm25;
        private final Double no2;

        private Event(String eventTime, String sensorId, Double pm25, Double no2) {
            this.eventTime = eventTime;
            this.sensorId = sensorId;
            this.pm25 = pm25;
            this.no2 = no2;
        }

        public String getEventTime() {
            return eventTime;
        }

        public String getSensorId() {
            return sensorId;
        }

        public Double getPm25() {
            return pm25;
        }

        public Double getNo2() {
            return no2;
        }
    }
}
