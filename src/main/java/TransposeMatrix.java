import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransposeMatrix {
    

    //Source for the output in csv : http://johnnyprogrammer.blogspot.fr/2012/01/custom-file-output-in-hadoop.html


    // In order to outut a csv we need to define an output class.
    public static class OutputInCsv extends FileOutputFormat<IntWritable, Text> {
        public OutputInCsv(){
            super();
        }

        @Override
        public org.apache.hadoop.mapreduce.RecordWriter<IntWritable, Text> getRecordWriter(TaskAttemptContext arg0)
                throws IOException, InterruptedException {
            Path path = FileOutputFormat.getOutputPath(arg0);
            Path fullPath = new Path(path, "output.csv");

            //create the file in the file system
            FileSystem fs = path.getFileSystem(arg0.getConfiguration());
            FSDataOutputStream fileOut = fs.create(fullPath, arg0);

            //create our record writer with the new file
            return new RecordWriterInCsv(fileOut);
        }
    }


    // We also need to define the writer config class
    public static class RecordWriterInCsv extends RecordWriter<IntWritable, Text> {
        private DataOutputStream out;

        public RecordWriterInCsv(DataOutputStream stream) {
            out = stream;
        }

        @Override
        public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
            out.close();
        }

        // Writes the line as string in the csv
        @Override
        public void write(IntWritable arg0, Text arg1) throws IOException, InterruptedException {
            out.writeBytes(arg1.toString());
            out.writeBytes("\r\n");
        }
    }




    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "pivot");

        //SET CLASSES
        job.setJarByClass(TransposeMatrix.class);

        //INPUT
        FileInputFormat.addInputPath(job, new Path(args[0]));


        //MAPPER
        job.setMapperClass(TransposeMatrixMapper.class);

        //REDUCER
        job.setReducerClass(TransposeMatrixReducer.class);

        //OUTPUT
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));



        //OUTPUTINCSV
        job.setOutputFormatClass(OutputInCsv.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}


