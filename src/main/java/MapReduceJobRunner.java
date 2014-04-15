import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created with IntelliJ IDEA.
 * User: fhp
 * Date: 4/14/14
 * Time: 2:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class MapReduceJobRunner {


    public MapReduceJobRunner() {}

    public void runMapReduceJob(String featureName,
                                Path mapredCSVFilePath,
                                String hdfsJarPath) throws Exception {
          Job job = Job.getInstance(new Configuration());

          Configuration conf = job.getConfiguration();

          job.setMapperClass(IngestToAccumuloMapper.class);
          job.setOutputFormatClass(AccumuloFileOutputFormat.class);

          job.setJobName("Ingesting tsv " + featureName + " to Accumulo.");

          FileSystem fs = FileSystem.get(conf);

          FileInputFormat.setInputPaths(job, mapredCSVFilePath);
          FileOutputFormat.setOutputPath(job, new Path("///tmp/hunter", "files"));

          for (FileStatus path : fs.listStatus(new Path(hdfsJarPath))) {
              job.addArchiveToClassPath(new Path(path.getPath().toUri().getPath()));
          }

          // both the indexing schema and the simple-feature type must go to the mapper
          //job.getConfiguration().set("featureName", featureName)



          job.submit();

          if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
          }
        }
}
