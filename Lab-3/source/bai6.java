package source;

import java.io.*;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai6 {

    private static int timestampToYear(long unixSeconds) {
        return Instant.ofEpochSecond(unixSeconds)
                      .atZone(ZoneId.of("UTC"))
                      .getYear();
    }

    public static class TimeRatingMapper extends Mapper<Object, Text, Text, Text> {

        private final Text yearKey     = new Text();
        private final Text ratingCount = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;
            if (line.toLowerCase().startsWith("userid")) return; 

            String[] fields = line.split(",");
            if (fields.length < 4) return;

            try {
                double rating    = Double.parseDouble(fields[2].trim());
                long   timestamp = Long.parseLong(fields[3].trim());

                int year = timestampToYear(timestamp);

                // Emit (Year, "rating,1")
                yearKey.set(String.valueOf(year));
                ratingCount.set(rating + ",1");
                context.write(yearKey, ratingCount);

            } catch (NumberFormatException e) {
                // pass
            }
        }
    }


    public static class TimeRatingReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double totalSum   = 0.0;
            long   totalCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length < 2) continue;

                try {
                    double rating = Double.parseDouble(parts[0].trim());
                    long   count  = Long.parseLong(parts[1].trim());
                    totalSum   += rating * count;
                    totalCount += count;
                } catch (NumberFormatException e) {
                    // pass
                }
            }

            double avgRating = (totalCount > 0) ? totalSum / totalCount : 0.0;

            String outputVal = String.format(
                    "Avg Rating: %.2f, Total Ratings: %d", avgRating, totalCount);
            context.write(key, new Text(outputVal));
        }
    }


    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: bai6 <ratings1> <ratings2> <o>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Time-Based Rating Analysis");

        job.setJarByClass(bai6.class);
        job.setMapperClass(TimeRatingMapper.class);
        job.setReducerClass(TimeRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, TimeRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, TimeRatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}