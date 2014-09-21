

import java.util.*;


/* Author: Luka Golinar */

/* This is the class containing Node information */

public class Node {
	
	public static enum Color {WHITE, GRAY, BLACK};
	
	/* Vertex */
	private final int id;
	/* Distance from source node */
	private int distance = Integer.MAX_VALUE; 
	/* List of all edges */
	private List<Integer> edges = new ArrayList<Integer>();
	/* List of all edges weight */
	private List<Integer> edges_weight = new ArrayList<Integer>();
	
	private Color color = Color.WHITE;
	
	public Node(int id) {
	    this.id = id;
	  }
	  
	  public int getId(){
	    return this.id;
	  }
	  
	  public int getDistance(){
	    return this.distance;
	  }
	  
	  public void setDistance(int distance) {
	    this.distance = distance;
	  }
	  
	  public Color getColor(){
	    return this.color;
	  }
	  
	  public void setColor(Color color){
	    this.color = color;
	  }
	  
	  public List<Integer> getEdges(){
	    return this.edges;
	  }
	  
	  public void setEdges(List<Integer> vertices) {
	    this.edges = vertices;
	  }
	  
	  public List<Integer> getWeights(){
		    return this.edges_weight;
		  }
		  
		  public void setWeight(List<Integer> weight) {
		    this.edges_weight = weight;
		  }
	  
	  public String toString(){
		  String ret_edes = "";
		  if(edges.size() > 0){
			  for (int i = 0; i < edges.size(); i++) {
					if(i != edges.size() - 1) ret_edes += edges.get(i).toString() + ",";
				    else
					  ret_edes += edges.get(i).toString() + " ";
				  } 
		  }else{
			  ret_edes = "NULL ";
		  }
		  
		  String ret_weight = "";
		  if(edges_weight.size() > 0){
			  for (int i = 0; i < edges_weight.size(); i++) {
					if(i != edges_weight.size() - 1) ret_edes += edges_weight.get(i).toString() + ",";
				    else
					  ret_edes += edges_weight.get(i).toString() + " ";
				  } 
		  }else{
			  ret_weight = "NULL ";
		  }
		  return ret_edes + ret_weight + Integer.toString(distance) + " " + color.toString();
	  }
}