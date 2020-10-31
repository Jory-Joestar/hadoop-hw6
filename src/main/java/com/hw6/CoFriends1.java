package com.hw6;
/*
思路：
input:
p1, f11 f12 f13...
p2, f21 f22 f23...
...
mapper输出：
<[fki,fkj],pk>
reducer输出:
<[fi,fj],[p1,p2,...]>
*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class CoFriends1 {

    static LinkedList<String> visited=new LinkedList<String>();
    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

        static enum CountersEnum { INPUT_WORDS }


        @Override
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            String line = value.toString();
            //删去逗号
            line = line.replaceAll(",", "");
            //以空格分割
            StringTokenizer itr = new StringTokenizer(line);
            //第一个用户编号作为后面用户的共同好友，设为value值
            String aFriend=itr.nextToken();
            //用一个列表visited记住所有用户。
            visited.add(aFriend);
            //将后面的用户加入列表，以方便遍历
            LinkedList<String> persons=new LinkedList<String>();
            while (itr.hasMoreTokens()) {
                persons.add(itr.nextToken());
            }
            //做两重循环，对任意两个不相同的用户，产生key值[Ui,Uj]
            int size=persons.size();
            for (int i=0;i<size;i++) {
                for (int j=i+1;j<size;j++) {
                    String p_touple="["+persons.get(i)+","+persons.get(j)+"]";
                    context.write(new Text(p_touple),new Text(aFriend));
                }
            }
        }

        @Override
        protected void cleanup(Context context
                            ) throws IOException, InterruptedException {
            //为了解决没有共同好友的用户组不输出的问题，在所有map结束之后
            //对任意两个不相同的用户产生一个<"[Ui,Uj]","*">的key-value对
            int size=visited.size();
            for (int i=0;i<size;i++) {
                for (int j=i+1;j<size;j++) {
                    String p_touple="["+visited.get(i)+","+visited.get(j)+"]";
                    context.write(new Text(p_touple),new Text("*"));
                }
            }
        }
    }


    public static class SumReducer
       extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
            //先将所有value存到一个TreeSet中，为了让输出的共同好友是按顺序排列的。
            TreeSet<String> cofriends= new TreeSet<String>();
            for (Text val:values) {
                cofriends.add(val.toString());
            }
            //还不要忘了删去Mapper的cleanup阶段产生的无意义value
            cofriends.remove("*");
            //拼接value字符串，将字符串转为特定的输出格式。
            String valuetuple="["+StringUtils.join(",", cofriends)+"]";
            String keyform="("+key.toString();
            String valueform=valuetuple+")";
            context.write(new Text(keyform),new Text(valueform));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "find cofriends");
        job.setJarByClass(CoFriends1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
