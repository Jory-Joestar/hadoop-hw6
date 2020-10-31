package com.hw6;
/*
思路：做2次mapreduce
第一次：
input:
p1,f11 f12 f13...
p2,f21 f22 f23...
...
注意，每个person都有一个空friends，作为所有人的共同好友（为了解决没有好友的人不会输出的问题）
mapper输出的k-v:
<fi,pj>
<"*",pj>
reducer输出的k-v:
<fi,[p1 p2 p3....]>
<"*",[p1,p2,p3,....,pn]>
第二次:
input:第一次的output
mapper输出的k-v:
<[pi,pj],f>
<[pi,pj],"*">
reducer输出的k-v:
<[pi,pj],[f1,f2,...]>
<[pi,pj],[""]>
*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.LinkedList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;

public class CoFriends2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    static enum CountersEnum { INPUT_WORDS }

    private Text person=new Text();
    private Text friend=new Text();

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      line = line.replaceAll(",", "");
      StringTokenizer itr = new StringTokenizer(line);
      String aPerson=itr.nextToken();
      person.set(aPerson);
      //为了解决没有共同好友的用户不输出的问题，假设每一个用户都关注一个空好友。
      friend.set("*");
      context.write(friend, person);
      //按好友列表切分，key为某个好友friendx，value为当前用户person
      while (itr.hasMoreTokens()) {
        friend.set(itr.nextToken());
        context.write(friend, person);
      }
    }
  }

  public static class SumReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      TreeSet<String> persons= new TreeSet<String>();
      //将关注同一个friend的person合成一个string
      for (Text val:values) {
        persons.add(val.toString());
      }
      context.write(key, new Text(StringUtils.join(",", persons)));
    }
  }

  public static class CombineMapper
       extends Mapper<Text, Text, Text, Text>{

    static enum CountersEnum { INPUT_WORDS }

    private Text personTuple=new Text();

    @Override
    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      line = line.replaceAll(",", " ");
      StringTokenizer itr = new StringTokenizer(line);
      Text coFriend=key;

      LinkedList<String> persons=new LinkedList<String>();

      while (itr.hasMoreTokens()) {
        persons.add(itr.nextToken());
      }
      int size=persons.size();
      for (int i=0;i<size;i++) {
        for (int j=i+1;j<size;j++) {
          String p_touple="["+persons.get(i)+","+persons.get(j)+"]";
          personTuple.set(p_touple);
          context.write(personTuple,coFriend);
        }
      }
    }
  }

  public static class CombineReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      TreeSet<String> friends= new TreeSet<String>();
      for (Text val:values) {
        friends.add(val.toString());
      }
      friends.remove("*");
      String valuetuple="["+StringUtils.join(",", friends)+"]";
      String keyform="("+key.toString();
      String valueform=valuetuple+")";
      context.write(new Text(keyform),new Text(valueform));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "find cofriends");
    job.setJarByClass(CoFriends2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path tempDir = new Path("cofriends-temp-" + Integer.toString(
                    new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录
    FileOutputFormat.setOutputPath(job, tempDir);  //将第一个job的结果写到临时目录中，下一个任务把临时目录作为输入目录
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    if (job.waitForCompletion(true)){
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job combineJob = Job.getInstance(conf,"combine");

        combineJob.setJarByClass(CoFriends2.class);
        FileInputFormat.addInputPath(combineJob, tempDir);
        combineJob.setInputFormatClass(SequenceFileInputFormat.class);

        combineJob.setMapperClass(CombineMapper.class);
        combineJob.setReducerClass(CombineReducer.class);
        
        FileOutputFormat.setOutputPath(combineJob,new Path(args[1]));

        combineJob.setOutputKeyClass(Text.class);
        combineJob.setOutputValueClass(Text.class);

        System.exit(combineJob.waitForCompletion(true) ? 0 : 1);

    } else {
        System.exit(1);
    }
  }
}