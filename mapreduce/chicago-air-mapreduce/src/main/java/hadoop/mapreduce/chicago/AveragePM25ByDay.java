package hadoop.mapreduce.chicago;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AveragePM25ByDay {

    /*
     * Update these indexes after inspecting the CSV header if the Chicago portal
     * schema changes.
     *
     * Current dataset header used for this project:
     * 0  datasourceid
     * 1  time
     * 22 no2ConcIndividual.value
     * 24 pm2_5ConcMassIndividual.value
     * 39 latitude
     * 40 longitude
     *
     * Common aliases:
     * - time: time, timestamp, measurement_time, date, measurement date
     * - sensor: datasourceid, sensor_id, sensor id, node id, site id
     * - PM2.5: pm2_5ConcMassIndividual.value, pm2.5, pm25, pm2_5, pm2.5 value
     * - NO2: no2ConcIndividual.value, no2, no2 value, no2.value
     */
    public static final int TIMESTAMP_COL_INDEX = 1;
    public static final int SENSOR_COL_INDEX = 0;
    public static final int PM25_COL_INDEX = 24;
    public static final int NO2_COL_INDEX = 22;

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

    public static class DoubleCountWritable implements Writable {
        private double sum;
        private long count;

        public DoubleCountWritable() {
        }

        public DoubleCountWritable(double sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public double getSum() {
            return sum;
        }

        public long getCount() {
            return count;
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeLong(count);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            sum = in.readDouble();
            count = in.readLong();
        }
    }

    public static class AveragePM25Mapper
            extends Mapper<LongWritable, Text, Text, DoubleCountWritable> {

        private final Text outputKey = new Text();
        private final DoubleCountWritable outputValue = new DoubleCountWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) {
                return;
            }

            String[] columns = parseCsvLine(line);
            if (columns.length == 0 || looksLikeHeader(columns)) {
                return;
            }

            if (columns.length <= Math.max(TIMESTAMP_COL_INDEX, PM25_COL_INDEX)) {
                context.getCounter("ChicagoAirQuality", "ShortRows").increment(1L);
                return;
            }

            String date = extractDate(columns[TIMESTAMP_COL_INDEX]);
            Double pm25 = parseDouble(columns[PM25_COL_INDEX]);
            if (date == null || pm25 == null) {
                context.getCounter("ChicagoAirQuality", "InvalidPM25Rows").increment(1L);
                return;
            }

            outputKey.set(date);
            outputValue.sum = pm25;
            outputValue.count = 1L;
            context.write(outputKey, outputValue);
        }
    }

    public static class SumCombiner
            extends Reducer<Text, DoubleCountWritable, Text, DoubleCountWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleCountWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0L;

            for (DoubleCountWritable value : values) {
                sum += value.getSum();
                count += value.getCount();
            }

            context.write(key, new DoubleCountWritable(sum, count));
        }
    }

    public static class AverageReducer
            extends Reducer<Text, DoubleCountWritable, Text, DoubleWritable> {

        private final DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleCountWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0L;

            for (DoubleCountWritable value : values) {
                sum += value.getSum();
                count += value.getCount();
            }

            if (count > 0) {
                result.set(sum / count);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println(
                    "Usage: AveragePM25ByDay <input_path> <output_path>");
            System.exit(1);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Average PM2.5 By Day");
        job.setJarByClass(AveragePM25ByDay.class);

        job.setMapperClass(AveragePM25Mapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(AverageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleCountWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static String[] parseCsvLine(String line) throws IOException {
        return CsvUtils.parseLine(line);
    }

    private static boolean looksLikeHeader(String[] columns) {
        return columns.length > Math.max(TIMESTAMP_COL_INDEX, SENSOR_COL_INDEX)
                && normalize(columns[SENSOR_COL_INDEX]).equals("datasourceid")
                && normalize(columns[TIMESTAMP_COL_INDEX]).equals("time");
    }

    private static String normalize(String value) {
        return value == null
                ? ""
                : value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "");
    }

    private static Double parseDouble(String value) {
        if (value == null) {
            return null;
        }

        String cleaned = value.trim();
        if (cleaned.isEmpty() || cleaned.equalsIgnoreCase("null")) {
            return null;
        }

        try {
            return Double.parseDouble(cleaned);
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private static String extractDate(String timestampValue) {
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
}
