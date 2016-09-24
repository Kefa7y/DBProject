package bplusTree;

public class Bplus {

	private Node root;
	public String tableName;
	public static int n;
	
	public Bplus(String name){
		tableName=name;
	}
	
	public void insert(Comparable x,Integer pNum , Integer index){
		if(root==null)
			root=new Leaf(null);
		NonLeafValue r=root.add(x , pNum,index);
		if(r!=null){
			NonLeaf nf = new NonLeaf();
			nf.values.add(r);
			nf.lastPointer = r.pointer;
			r.pointer = root;	
			root = nf;
		}
	}
	
	public LeafValue find(Integer x){
		return root.find(x);
	}
	
	public void delete(Integer x){
		LeafValue lv = find(x);
		if(lv!=null)
			lv.exists= false;
	}
}
