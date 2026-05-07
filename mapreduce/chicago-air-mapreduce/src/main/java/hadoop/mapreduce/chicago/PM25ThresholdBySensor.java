package hadoop.mapreduce.chicago;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PM25ThresholdBySensor {

    /*
     * Update these indexes after inspecting the CSV header if the dataset schema
     * changes.
     *
     * Current project header:
     * 0  datasourceid
     * 1  time
     * 22 no2ConcIndividual.value
     * 24 pm2_5ConcMassIndividual.value
     *
     * Common aliases:
     * - sensor: datasourceid, sensor_id, sensor id, node id, site id
     * - PM2.5: pm2_5ConcMassIndividual.value, pm2.5, pm25, pm2_5, pm2.5 value
     */
    public static final int TIMESTAMP_COL_INDEX = 1;
    public static final int SENSOR_COL_INDEX = 0;
    public static final int PM25_COL_INDEX = 24;
    public static final int NO2_COL_INDEX = 22;
    public static final double PM25_THRESHOLD = 35.0;

    public static class ThresholdMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text outputKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().isEmpty()) {
                return;
            }

            AirQualityEventParser.Event event = AirQualityEventParser.parse(
                    line,
                    TIMESTAMP_COL_INDEX,
                    SENSOR_COL_INDEX,
                    PM25_COL_INDEX,
                    NO2_COL_INDEX);
            if (event == null) {
                context.getCounter("ChicagoAirQuality", "InvalidInputRows").increment(1L);
                return;
            }

            String sensorId = event.getSensorId();
            Double pm25 = event.getPm25();
            if (sensorId == null || sensorId.isEmpty() || pm25 == null) {
                context.getCounter("ChicagoAirQuality", "InvalidPM25Rows").increment(1L);
                return;
            }

            if (pm25 > PM25_THRESHOLD) {
                outputKey.set(sensorId);
                context.write(outputKey, ONE);
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println(
                    "Usage: PM25ThresholdBySensor <input_path> <output_path>");
            System.exit(1);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "PM2.5 Threshold By Sensor");
        job.setJarByClass(PM25ThresholdBySensor.class);

        job.setMapperClass(ThresholdMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
