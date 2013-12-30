package detect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

public class Status {

	private HashMap<Integer, Integer> candidate;
	private HashSet<Integer> notset;
	private ArrayList<Integer> result;
	private int level;
	private int vp;
	HashMap<Integer, HashSet<Integer>> deg2cand;
	TreeMap<Integer, HashSet<Integer>> od2c;

	public Status(String s) {
		this.read(s);
	}

	public Status(int tmpKey, int i, HashMap<Integer, Integer> vertex,
			HashSet<Integer> tnot,
			HashMap<Integer, HashSet<Integer>> deg2cand2,
			TreeMap<Integer, HashSet<Integer>> od) {
		vp = tmpKey;
		level = i;
		candidate = vertex;
		notset = tnot;
		deg2cand = deg2cand2;
		od2c = od;
	}

	public ArrayList<Integer> getResult() {
		return result;
	}

	public void setResult(ArrayList<Integer> result) {
		this.result = result;
	}

	public int getVp() {
		return vp;
	}

	public TreeMap<Integer, HashSet<Integer>> getOd2c() {
		return od2c;
	}

	public void setOd2c(TreeMap<Integer, HashSet<Integer>> od2c) {
		this.od2c = od2c;
	}

	public void setVp(int vp) {
		this.vp = vp;
	}

	public void setCandidate(HashMap<Integer, Integer> init) {
		this.candidate = init;
	}

	public HashMap<Integer, Integer> getCandidate() {
		return this.candidate;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public HashMap<Integer, HashSet<Integer>> getDeg2cand() {
		return deg2cand;
	}

	public void setDeg2cand(HashMap<Integer, HashSet<Integer>> deg2cand) {
		this.deg2cand = deg2cand;
	}

	public HashSet<Integer> getNotset() {
		return notset;
	}

	public void setNotset(HashSet<Integer> notset) {
		this.notset = notset;
	}

	public String toString(List<Integer> result) {
		StringBuilder re = new StringBuilder();
		re.append(vp + "%" + level + "%");
		String key = "";
		key = this.candidate.keySet().toString();
		re.append(candidate.size());
		re.append(",");// cand size
		re.append(key.substring(1, key.length() - 1));// cand content
		re.append("%");

		key = notset.toString();
		re.append(notset.size());
		re.append(",");// notset size
		re.append(key.substring(1, key.length() - 1));// notset content
		if(level > 1){
			re.append("%");
			if(this.result!=null && this.result.size()>0){
				for(int i = 0;i<this.level-1;i++){
					re.append(this.result.get(i)+",");//result content
				}
			}else{
				for(int i = 0;i<this.level-1;i++){
					re.append(result.get(i)+",");//result content
				}
			}
		}
		return re.toString();
	}

	public void read(String s) {
		//System.out.println(s);
		String[] elms = s.split("%");
		this.vp = Integer.parseInt(elms[0]);
		this.level = Integer.parseInt(elms[1]);
		String candStr = elms[2];
		String[] ens = candStr.split(",");
		int candsize = Integer.parseInt(ens[0]);
		this.candidate = new HashMap<Integer, Integer>(candsize);
		for (int i = 1; i < ens.length; i++) {
			this.candidate.put(Integer.parseInt(ens[i].trim()), 0);
		}
		String notStr = elms[3];
		String[] enls = notStr.split(",");
		int notsize = Integer.parseInt(enls[0]);
		this.notset = new HashSet<Integer>(notsize);
		for (int i = 1; i < enls.length; i++) {
			this.notset.add(Integer.parseInt(enls[i].trim()));
		}
		this.result = new ArrayList<Integer>(this.level+this.candidate.size()/2);
		if(level>1){
			enls = elms[4].split(",");
			//int resultsize = Integer.parseInt(enls[0]);
			for(int i = 0;i<this.level-1;i++){
				this.result.add(Integer.parseInt(enls[i]));
			}
			
		}
	}
}
