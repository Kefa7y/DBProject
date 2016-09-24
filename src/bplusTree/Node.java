package bplusTree;


public interface Node{
	public NonLeafValue add(Comparable value, Integer pNum , Integer index);
	public boolean overflow();
	public String toString();
	public LeafValue find(Comparable x);
}
