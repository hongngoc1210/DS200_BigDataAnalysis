package source;
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class bai2 {
    public static class GenreRatingMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Map<String, String> movieGenreMap = new HashMap<>();
        private final Text genreKey = new Text();
        private final DoubleWritable ratingValue = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) return;
            String fileName = new Path(cacheFiles[0].toString()).getName();

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(fileName)))) {

                String line;
                boolean firstLine = true;

                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    if (firstLine && line.toLowerCase().contains("movieid")) {
                        firstLine = false;
                        continue;
                    }
                    firstLine = false;

                    String[] parts = line.split(",");
                    if (parts.length >= 3) {
                        String movieId = parts[0].trim();
                        String genres = parts[parts.length - 1].trim();
                        movieGenreMap.put(movieId, genres);
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            if (line.toLowerCase().startsWith("userid")) return;

            String[] parts = line.split(",");
            if (parts.length < 4) return;

            try {
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                String genres = movieGenreMap.get(movieId);
                if (genres == null || genres.isEmpty()) return;

                String[] genreList = genres.split("\\|");
                ratingValue.set(rating);

                for (String genre : genreList) {
                    genre = genre.trim();
                    if (!genre.isEmpty()) {
                        genreKey.set(genre);
                        context.write(genreKey, ratingValue);
                    }
                }
            } catch (NumberFormatException e) {
            }
        }
    }

    public static class GenreRatingReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            long totalRating = 0;
            double sumRating = 0.0;

            for (DoubleWritable value : values) {
                sumRating += value.get();
                totalRating++;
            }

            double avgRating = (totalRating > 0) ? (sumRating / totalRating) : 0.0;

            String outputVal = String.format("Avg Rating: %.2f, Total Ratings: %d", avgRating, totalRating);
            context.write(key, new Text(outputVal));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: bai2 <ratings_1> <ratings_2> <movies> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Average Rating");
        job.setJarByClass(bai2.class);

        job.setMapperClass(GenreRatingMapper.class);
        job.setReducerClass(GenreRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GenreRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, GenreRatingMapper.class);

        job.addCacheFile(new Path(args[2]).toUri());

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}