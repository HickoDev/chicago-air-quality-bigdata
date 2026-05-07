package hadoop.mapreduce.chicago;

import java.io.IOException;
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

public class AverageNO2BySensor {

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
     * - NO2: no2ConcIndividual.value, no2, no2 value, no2.value
     */
    public static final int TIMESTAMP_COL_INDEX = 1;
    public static final int SENSOR_COL_INDEX = 0;
    public static final int PM25_COL_INDEX = 24;
    public static final int NO2_COL_INDEX = 22;

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

    public static class AverageNO2Mapper
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
            Double no2Value = event.getNo2();
            if (sensorId == null || sensorId.isEmpty() || no2Value == null) {
                context.getCounter("ChicagoAirQuality", "InvalidNO2Rows").increment(1L);
                return;
            }

            outputKey.set(sensorId);
            outputValue.sum = no2Value;
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
                    "Usage: AverageNO2BySensor <input_path> <output_path>");
            System.exit(1);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Average NO2 By Sensor");
        job.setJarByClass(AverageNO2BySensor.class);

        job.setMapperClass(AverageNO2Mapper.class);
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
}
