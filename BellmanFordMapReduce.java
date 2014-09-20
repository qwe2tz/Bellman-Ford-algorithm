
import java.io.IOError;
import java.io.IOException;
import java.util.*;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jasper.tagplugins.jstl.core.Out;
import org.apache.log4j.NDC;




public class BellmanFordMapReduce extends Configured implements Tool{
	
	public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.BellmanFordMapReduce");
	
	public static class BF_Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
		
		
		/* MAP FUNCTION */
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			
			/* Parse the new line */
			String[] nodeValue = value.toString().split(" ");
			String[] edges = nodeValue[1].split(",");
			
			/* Make a new Node from acquired data */
			Node node = new Node(Integer.parseInt(nodeValue[0]));
			
			/* Create all the edges from the node */
			List<Integer> e = new ArrayList<Integer>();
			for (int i = 1; i < edges.length; i++) {
				System.out.println(edges[i]);
				e.add(Integer.parseInt(edges[i]));
			}
			/* Setting edges */
			node.setEdges(e);
			
			/* Setting node color */
			Node.Color color = Node.Color.WHITE;
			switch(nodeValue[3]){
				case "GRAY":
					color = Node.Color.GRAY;
				case "WHITE":
					color = Node.Color.WHITE;
				case "BLACK":
					color = Node.Color.BLACK;
			}
			node.setColor(color);
			
			/* Set distance */
			node.setDistance(Integer.parseInt(nodeValue[3]));
			
			/* If the node is GREY, we emit all of the node edges */
		      if (node.getColor() == Node.Color.GRAY) {
		        for (int v : node.getEdges()) {
		          Node vnode = new Node(v);
		          /* Set the distance of the edges -- Main BellmanFord Algorithm */
		          if(vnode.getDistance() < node.getDistance() + node.getId()){
		        	  vnode.setDistance(node.getDistance() + node.getId());
		          }
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
				LOG.info("Reduce executing for input key [" + key.toString() + "]");
				
				List<Integer> edges = null;
			    int distance = Integer.MAX_VALUE;
			    Node.Color color = Node.Color.WHITE;
			    
			    while(values.hasNext()){
			    	Text value = values.next();
			    	
			    	/* Create a new node from the given key and value TODO -- Make the code better by putting the argument parse in the Node class */
			    	Node node = new Node(key.get());
			    	String[] nodeValue = value.toString().split(" ");
					String[] ed = nodeValue[1].split(",");
					
			    	List<Integer> e = new ArrayList<Integer>();
			    	
					for (int i = 0; i < ed.length; i++) {
						e.add(Integer.parseInt(ed[i]));
					}
					/* Setting edges */
					node.setEdges(e);
					
					/* Setting node color */
					Node.Color c = Node.Color.WHITE;
					switch(nodeValue[3]){
						case "GRAY":
							color = Node.Color.GRAY;
						case "WHITE":
							color = Node.Color.WHITE;
						case "BLACK":
							color = Node.Color.BLACK;
					}
					node.setColor(color);
					
					/* Set distance */
					node.setDistance(Integer.parseInt(nodeValue[3]));
			    	
					/* Only one node will be expanded */
					if(node.getEdges().size() > 0) edges = node.getEdges();
					
					if(node.getDistance() < distance) distance = node.getDistance();
					
					if(node.getColor().ordinal() > color.ordinal()) color = node.getColor();
					
					Node n = new Node(key.get());
					n.setDistance(distance);
					n.setEdges(edges);
					n.setColor(color);
					
					/* Emit the node */
					output.collect(key, new Text(n.toString()));
					
					/* Log info */
					LOG.info("Reduce outputting final key [" + key + "] and value [" + n.toString() + "]");
			    }
			}
		}
}

/* TODO: Implement the the Jobs */
		
		


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

