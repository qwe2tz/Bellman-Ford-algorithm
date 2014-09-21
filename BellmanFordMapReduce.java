import java.io.IOError;
import java.io.IOException;
import java.util.*;

import javax.naming.Context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileInputFormat.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jasper.tagplugins.jstl.core.Out;
import org.apache.log4j.NDC;





public class BellmanFordMapReduce extends Configured implements Tool{
	
	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.BellmanFordMapReduce");
	
	public enum bf_counter { Counter }
	
	public static class BF_Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
		
		
		/* MAP FUNCTION */
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			
			/* Parse the new line */
			String[] nodeValue = value.toString().replace("\t", " ").split(" ");
			String[] edges = nodeValue[1].split(",");
			String[] weight = nodeValue[2].split(",");
			
			/* Make a new Node from acquired data */
			Node node = new Node(Integer.parseInt(nodeValue[0]));
			
			/* Create all the edges from the node */
			List<Integer> e = new ArrayList<Integer>();
			for (int i = 0; i < edges.length; i++) {
				e.add(Integer.parseInt(edges[i]));
			}
			/* Setting edges */
			node.setEdges(e);
			
			/* Create all the edges from the node */
			List<Integer> w = new ArrayList<Integer>();
			for (int i = 0; i < weight.length; i++) {
				w.add(Integer.parseInt(weight[i]));
			}
			/* Setting edges */
			node.setWeight(w);
			
			/* Setting node color */
			node.setColor(Node.Color.valueOf(nodeValue[4]));
			
			/* Set distance */
			if(nodeValue[3].equals("Integer.MAX_VALUE")) node.setDistance(Integer.MAX_VALUE);
			else node.setDistance(Integer.parseInt(nodeValue[3]));
			
			/* If the node is GREY, we emit all of the node edges */
		      if (node.getColor() == Node.Color.GRAY) {
		    	int counter = 0;
		        for (int v : node.getEdges()) {
		          Node vnode = new Node(v);
		          LOG.info("Weights size = " + node.getWeights().size());
		          int w_e = node.getWeights().get(counter);
		          /* Set the distance of the edges -- Main BellmanFord Algorithm */
		          if(vnode.getDistance() > node.getDistance() + w_e) vnode.setDistance(node.getDistance() + w_e);
		          
		          
		          counter++;
		          
		          /* Set node as visited */
		          vnode.setColor(Node.Color.GRAY);
		          output.collect(new IntWritable(vnode.getId()), new Text(vnode.toString()));
		        }
		        /* We're done with this node now, color it BLACK */
		        node.setColor(Node.Color.BLACK);
		      }

		     /* We always emit the input node. If it was GREY, we color it BLACK */
		      output.collect(new IntWritable(node.getId()), new Text(node.toString()));
		      
		    }
		}
		
		/* REDUCE FUNCTION */
		public static class BF_Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{
			
			public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter report) throws IOException{
				LOG.info("Reduce executing for input node [" + key.toString() + "]");
				
				List<Integer> edges = null;
				List<Integer> weights = null;
			    int distance = Integer.MAX_VALUE;
			    Node.Color color = Node.Color.WHITE;
			    
			    while(values.hasNext()){
			    	Text value = values.next();
			    	
			    	/* Create a new node from the given key and value TODO -- Make the code better by putting the argument parse in the Node class 
			    	 * Key = 2	Value = 1,3,4,5 w1,w2,w3,w4 Integer.MAX_VALUE WHITE */
			    	Node node = new Node(key.get());
			    	String[] nodeValue = value.toString().split(" ");
			    	LOG.info("NODE VALUE LENGTH = " + nodeValue.length);
					String[] ed = nodeValue[0].split(",");
					String[] we = nodeValue[1].split(",");
					
			    	List<Integer> e = new ArrayList<Integer>();
			    	
					for (int i = 0; i < ed.length; i++) {
						if(ed[i].equals("NULL")) break;
						e.add(Integer.parseInt(ed[i]));
					}
					
					List<Integer> w = new ArrayList<Integer>();
					for (int i = 0; i < we.length; i++) {
						if(we[i].equals("NULL")) break;
						w.add(Integer.parseInt(we[i]));
					}
					/* Setting edges */
					node.setEdges(e);
					node.setWeight(w);
					/* Setting node color */
					LOG.info("NODE VALUE = " + nodeValue[0] + " " + nodeValue[1] + " " + nodeValue[2]);
					node.setColor(Node.Color.valueOf(nodeValue[3]));
										
					/* Set distance */
					node.setDistance(Integer.parseInt(nodeValue[2]));
			    	
					/* Only one node will be expanded */
					if(node.getEdges().size() > 0) edges = node.getEdges();
					
					if(node.getWeights().size() > 0) weights = node.getWeights();
					
					if(node.getDistance() < distance) distance = node.getDistance();
					
					if(node.getColor().ordinal() > color.ordinal()) {
						color = node.getColor();
					}
					
			    }
			    Node n = new Node(key.get());
				n.setDistance(distance);
				n.setEdges(edges);
				n.setWeight(weights);
				n.setColor(color);
				
				
				/* Increase the counter if the Node is GREY and we still need some work to do */
				if(n.getColor().equals("GREY")) {
					
				}

				/* Emit the node */
				output.collect(key, new Text(n.toString()));
				
				/* Log info for console debugging */
				LOG.info("Reduce outputting final node [" + key + "] and value [" + n.toString() + "]");
			}
		}
		
		  private JobConf getJobConf(String[] args) {
		    JobConf conf = new JobConf(getConf(), BellmanFordMapReduce.class);
		    conf.setJobName("BellmanFord Algorithm");

		    /* Keys are nodes unique identifiers */
		    conf.setOutputKeyClass(IntWritable.class);
		    
		    /* Node values are represented as a string */
		    conf.setOutputValueClass(Text.class);

		    conf.setMapperClass(BF_Map.class);
		    conf.setReducerClass(BF_Reduce.class);

		    for (int i = 0; i < args.length; i++) {
		      if ("-m".equals(args[i])) {
		        conf.setNumMapTasks(Integer.parseInt(args[i++]));
		      } else if ("-r".equals(args[i])) {
		        conf.setNumReduceTasks(Integer.parseInt(args[i]));
		      }
		    }

		    LOG.info("The number of reduce tasks has been set to " + conf.getNumReduceTasks());
		    LOG.info("The number of mapper tasks has been set to " + conf.getNumMapTasks());

		    return conf;
		  }

		  public int run(String[] args) throws Exception {

		    int iterationCount = 0;
		    /* TODO: Use a dinamic counter instead of a check method */
		    int terminate = 3;
		    while (terminate > 0) {

		      String input;
		      if (iterationCount == 0)
		        input = "input-graph";
		      else
		        input = "output-" + iterationCount;

		      String output = "output-" + (iterationCount + 1);

		      JobConf conf = getJobConf(args);
		      FileInputFormat.setInputPaths(conf, new Path(input));
		      FileOutputFormat.setOutputPath(conf, new Path(output));
		      RunningJob job = JobClient.runJob(conf);

		      iterationCount++;
		      terminate--;
		      //job.getCounters().findCounter(bf_counter.Counter).getValue();
		    }
		    
		    return 0;
		  }
		  
		  
		 

		  public static void main(String[] args) throws Exception {
		    int res = ToolRunner.run(new Configuration(), new BellmanFordMapReduce(), args);
		    System.exit(res);
		  }
	}





	/*
	 * Main bellmanford algorithm -- Unparallel version
	 * 
	 
	public static void BellmanFordAlgorithm(Vector<Node> edges, int nnodes, int source) {

		int[] distance = new int[nnodes];

		for (int i = 0; i < distance.length; i++) {
			distance[i] = Integer.MAX_VALUE;
		}
		distance[source] = 0;

		for (int i = 0; i < nnodes; i++) {
			for (int j = 0; j < edges.size(); j++) {
				if (distance[edges.get(j).source] == Integer.MAX_VALUE)
					continue;
				
				if (distance[edges.get(j).source] + edges.get(j).weight < distance[edges.get(j).destination]) {
					distance[edges.get(j).destination] = distance[edges.get(j).source] + edges.get(j).weight;
				}
			}
		}

		for (int i = 0; i < edges.size(); i++) {
			if (distance[edges.get(i).source] != Integer.MAX_VALUE && distance[edges.get(i).destination] > distance[edges.get(i).source] + edges.get(i).weight) {
				System.out.println("Negative edge weight cycles detected!");
				return;
			}
		}
	}
	
	
	 public static void main(String[] args) {
         Vector<Node> edges = new Vector<Node>();
         edges.add(new Node(0, 1, 5));
         edges.add(new Node(0, 2, 8));
         edges.add(new Node(0, 3, -4));
         edges.add(new Node(1, 0, -2));
         edges.add(new Node(2, 1, -3));
         edges.add(new Node(2, 3, 9));
         edges.add(new Node(3, 1, 7));
         edges.add(new Node(3, 4, 2));
         edges.add(new Node(4, 0, 6));
         edges.add(new Node(4, 2, 7));
         BellmanFordAlgorithm(edges, 5, 4);
}
*/

