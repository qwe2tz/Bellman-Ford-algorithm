

import java.util.*;


/* Author: Luka Golinar */

/* This is the class containing Node information */

public class Node {
	
	public static enum Color {WHITE, GRAY, BLACK};
	
	private final int id;
	private int parent = Integer.MAX_VALUE; 
	private int distance = Integer.MAX_VALUE; 
	private List<Integer> edges = null;
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
	  
	  public String toString(){
		  String ret_edes = "";
		  
		  
		  
		  for (int i = 0; i < edges.size(); i++) {
			if(i != edges.size() - 1) ret_edes += edges.get(i).toString() + ",";
		    else
			  ret_edes += edges.get(i).toString() + " ";
		  }
		  return ret_edes + Integer.toString(distance) + " " + color.toString();
	  }
}