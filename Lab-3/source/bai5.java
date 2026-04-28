package source;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

public class bai5 {

    public static class OccupationRatingMapper
            extends Mapper<Object, Text, Text, Text> {

        // UserID → OccupationID
        private final Map<String, String> userOccupationMap = new HashMap<>();

        // OccupationID → OccupationName
        private final Map<String, String> occupationNameMap = new HashMap<>();

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            String usersPath = conf.get("users.path");
            String occupationsPath = conf.get("occupations.path");

            FileSystem fs = FileSystem.get(conf);

            // =========================
            // READ users.txt
            // =========================
            try (
                FSDataInputStream in = fs.open(new Path(usersPath));
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(in))
            ) {

                String line;

                while ((line = br.readLine()) != null) {

                    line = line.trim();

                    if (line.isEmpty()) {
                        continue;
                    }

                    // bỏ header nếu có
                    if (line.toLowerCase().startsWith("userid")) {
                        continue;
                    }

                    // UserID,Gender,Age,OccupationID,Zip
                    String[] parts = line.split(",");

                    if (parts.length >= 4) {

                        String userId = parts[0].trim();
                        String occupationId = parts[3].trim();

                        userOccupationMap.put(userId, occupationId);
                    }
                }
            }

            // =========================
            // READ occupation.txt
            // =========================
            try (
                FSDataInputStream in = fs.open(new Path(occupationsPath));
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(in))
            ) {

                String line;

                while ((line = br.readLine()) != null) {

                    line = line.trim();

                    if (line.isEmpty()) {
                        continue;
                    }

                    // bỏ header nếu có
                    if (line.toLowerCase().startsWith("occupationid")
                            || line.toLowerCase().startsWith("id")) {
                        continue;
                    }

                    // OccupationID,OccupationName
                    String[] parts = line.split(",", 2);

                    if (parts.length >= 2) {

                        String occId = parts[0].trim();
                        String occName = parts[1].trim();

                        occupationNameMap.put(occId, occName);
                    }
                }
            }

            System.out.println("Loaded Users: " + userOccupationMap.size());
            System.out.println("Loaded Occupations: " + occupationNameMap.size());
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            // bỏ header nếu có
            if (line.toLowerCase().startsWith("userid")) {
                return;
            }

            // UserID,MovieID,Rating,Timestamp
            String[] parts = line.split(",");

            if (parts.length < 3) {
                return;
            }

            try {

                String userId = parts[0].trim();

                double rating =
                        Double.parseDouble(parts[2].trim());

                // lookup occupation id
                String occupationId =
                        userOccupationMap.get(userId);

                if (occupationId == null) {
                    return;
                }

                // lookup occupation name
                String occupationName =
                        occupationNameMap.get(occupationId);

                if (occupationName == null) {
                    occupationName = "Unknown";
                }

                // emit
                outKey.set(occupationName);
                outValue.set(rating + ",1");

                context.write(outKey, outValue);

            } catch (Exception e) {
                // pass
            }
        }
    }

    public static class OccupationRatingReducer
            extends Reducer<Text, Text, Text, Text> {

        private final Text result = new Text();

        @Override
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            double totalRating = 0.0;
            int totalCount = 0;

            for (Text val : values) {

                String[] parts =
                        val.toString().split(",");

                if (parts.length < 2) {
                    continue;
                }

                try {

                    double rating =
                            Double.parseDouble(parts[0]);

                    int count =
                            Integer.parseInt(parts[1]);

                    totalRating += rating;
                    totalCount += count;

                } catch (Exception e) {
                    // pass
                }
            }

            if (totalCount == 0) {
                return;
            }

            double avgRating =
                    totalRating / totalCount;

            result.set(
                    String.format("Average Rating: %.2f\tTotal Ratings: %d",
                            avgRating,
                            totalCount)
            );

            context.write(key, result);
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length < 5) {

            System.err.println(
                    "Usage: bai5 <ratings1> <ratings2> <users> <occupations> <output>"
            );

            System.exit(1);
        }

        Configuration conf = new Configuration();

        conf.set("users.path", args[2]);
        conf.set("occupations.path", args[3]);

        Job job = Job.getInstance(
                conf,
                "bai5 - Rating Analysis by Occupation"
        );

        job.setJarByClass(bai5.class);

        job.setMapperClass(OccupationRatingMapper.class);
        job.setReducerClass(OccupationRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(
                job,
                new Path(args[0]),
                TextInputFormat.class,
                OccupationRatingMapper.class
        );

        MultipleInputs.addInputPath(
                job,
                new Path(args[1]),
                TextInputFormat.class,
                OccupationRatingMapper.class
        );

        Path outputPath = new Path(args[4]);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputPath)) {

            fs.delete(outputPath, true);

            System.out.println(
                    "[Bai5] Deleted old output directory: "
                            + outputPath
            );
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}