package bottleneck;

//改成开始写磁盘时候只写一份！shuffle时候散对应的份数，少一些写磁盘数据量较小时差别并不是很大！但是考虑数据量很多时有差别！
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import detect.Status;

//import cliquenew.VariableRecord;

public class BottleneckTimeAllReducer {
	public static HashMap<Integer, Integer> vertex = new HashMap<Integer, Integer>(
			3000);
	public static HashMap<Integer, HashSet<Integer>> verEdge = new HashMap<Integer, HashSet<Integer>>(
			3000);
	// public static HashMap<Integer, Integer> mark = new HashMap<Integer,
	// Integer>();
	public static Stack<Status> stack = new Stack<Status>();
	public static ArrayList<Integer> result = new ArrayList<Integer>(30);
	public static long number = 0;
	public static int levelNumber = 0;
	public static int totalPart = 36;
	public static int tmpKey = 0;
	public static int count = 0;
	public static boolean done = false;
	// public static ArrayList<Integer> thenode = new ArrayList<Integer>();//
	// 需要计算的节点集
	public static HashSet<Integer> thenode = new HashSet<Integer>();// 需要计算的节点集
	public static int cutNumber = 0; // 较大的计算单元，切割次数
	public static int cutNumberAfter = 0;
	public static int Strategy = 0;
	public static long TimeThreshold = 0;
	public static int NodeSN = 0;
	public static int sizeN = 0;
	public static HashSet<Integer> hs = new HashSet<Integer>();
	public static boolean balanceOrNot = false;
	public static long tPhase = 0;
	public static File dirRoot = new File("/home/youli/CliqueHadoop/");
	public static File serial = new File(dirRoot, "serialNumber.txt");
	public static RandomAccessFile raf = null;
	public static int which = 0;// 区分reduce的唯一编号，用于写数据文件
	public static int MaxOne = 3;// 最小是三角形，初始化时即初始化为三角形
	public static int randomSelect = 0;
	public static HashSet<Integer> parts = new HashSet<Integer>();
	public static int numReducer = 0;
	public static class DetectReducer extends
			Reducer<PairTypeInt, Text, IntWritable, Text> {
		HashSet<Integer> notset = new HashSet<Integer>();
		HashMap<Integer, Integer> cand = new HashMap<Integer, Integer>();
		HashMap<Integer, HashSet<Integer>> deg2cand = new HashMap<Integer, HashSet<Integer>>();
		TreeMap<Integer, HashSet<Integer>> od2 = new TreeMap<Integer, HashSet<Integer>>();

		private int maxdeg;
		private boolean isClique;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			numReducer = context.getNumReduceTasks();
			FileReader fr = new FileReader(new File(dirRoot,
					"trianglenew_NODE.txt"));
			BufferedReader bfr = new BufferedReader(fr);
			// 提取出准备要处理的节点列表
			String record = "";
			while ((record = bfr.readLine()) != null) {
				String[] adjInfos = record.split(" ");
				for (int i = 0; i < adjInfos.length; i++)
					thenode.add(Integer.valueOf(adjInfos[i]));
			}
			bfr.close();

			FileReader fr2 = new FileReader(new File(dirRoot,
					"trianglenew_CLIQUE.txt"));
			BufferedReader bfr2 = new BufferedReader(fr2);
			// 参数配置信息
			String record2 = "";
			if ((record2 = bfr2.readLine()) != null) {
				String[] adjInfos = record2.split(" ");
				totalPart = Integer.valueOf(adjInfos[0]);
				Strategy = Integer.valueOf(adjInfos[1]);
				TimeThreshold = Long.valueOf(adjInfos[2]);
				sizeN = Integer.valueOf(adjInfos[3]);
				randomSelect = Integer.valueOf(adjInfos[4]);
			}
			bfr2.close();

			// 确定该reduce的唯一编号
			RandomAccessFile rafcur = null;
			try {
				rafcur = new RandomAccessFile(serial, "rw");
				FileChannel fc = rafcur.getChannel();
				;
				FileLock fl = null;
				boolean got = false;// 获得编号
				while (true) {
					Thread.sleep(20);
					try {
						// fc = rafcur.getChannel();
						fl = fc.tryLock();
						if (fl != null)// 可以锁住
						{
							if (serial.length() == 0)// 空文件
							{
								rafcur.write("0\n".getBytes());
								which = 0;
							} else {
								String line = rafcur.readLine();
								which = (Integer.valueOf(line) + 1);
								rafcur.seek(0);
								rafcur.write((which + "\n").getBytes());
							}
							got = true;
						} else {
							Thread.sleep(20);// 避免try过多
						}
					} finally {
						// if(rafcur!=null)
						// rafcur.close();
						if (fl != null) {
							try {
								fl.release();
							} catch (Exception e) {
							}
						}
					}
					if (got)// 获得了唯一编号
						break;
				}
			} finally {
				if (rafcur != null)
					rafcur.close();
			}
			// 获得唯一编号后打开文件
			File curReduce = new File(dirRoot, which + "");
			raf = new RandomAccessFile(curReduce, "rw");
			//raf.write("setup ok".getBytes());
		}

		int numClique = 0;

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Stack<Status> stack = new Stack<Status>();
			// reduce运行结束，时间T还没有到，接着读取/home/dic/over/中的文件处理
			if (raf.getFilePointer() != 0 && tPhase < TimeThreshold) {// 文件中有内容
				verEdge.clear();
				stack.clear();
				result = null;
				raf.seek(0);
				// 重新写入这个文件，原来的文件作废，最后将其删除！
				RandomAccessFile rnew = new RandomAccessFile(new File(dirRoot,
						which + "#"), "rw");
				String line = "";
				String lastline = "";
				long t1 = System.currentTimeMillis();
				long t2 = System.currentTimeMillis();
				boolean gotenough = false;
				while ((line = raf.readLine()) != null) {
					// 是子图或者边邻接信息
					String[] ab = line.split("\t");
					if (Integer.parseInt(ab[0]) == -2) {
						// 是边邻接信息
						String b = ab[1];
						String[] vas = b.split("#");
						tmpKey = Integer.parseInt(vas[2]);
						String edgestring = vas[3];
						readVerEdge(edgestring, verEdge);
					} else {
						// 是子图信息
						String[] elements = ab[1].split("@");
						tmpKey = Integer.parseInt(elements[2]);
						Status st = new Status(elements[3]);
						stack.add(st);
						if (elements[0].equals("0")) {
							// 完整的子图包括verEdge
							readVerEdge(elements[4], verEdge);
						} else {
							// 仅子图信息
							continue;// 还没有读全，继续往下读
						}
					}

					// 一个完整的子图已经都读进来了，计算这个子图
					parts.clear();
					while (!stack.empty()) {
						Status top = stack.pop();
						HashSet<Integer> notset = top.getNotset();
						HashMap<Integer, Integer> cand = top.getCandidate();
						HashMap<Integer, HashSet<Integer>> d2c;
						TreeMap<Integer, HashSet<Integer>> od2c;
						int level = top.getLevel();
						int vp = top.getVp();
						if(top.getResult()!=null){
							result = top.getResult();
						}
						if(level+cand.size()<=MaxOne){
							continue;
						}
						if (allContained(cand, notset)) {
							continue;
						}
						if (result.size() + 1 == level) {
							result.add(vp);
						} else {
							result.set(level - 1, vp);
						}
						d2c = top.getDeg2cand();
						od2c = top.getOd2c();
						if (d2c == null) {
							d2c = new HashMap<Integer, HashSet<Integer>>();
							od2c = new TreeMap<Integer, HashSet<Integer>>();
							updateDeg(cand, d2c, od2c);
						}
						if (judgeClique(d2c)) {
							emitClique(result, level, cand, context);
							continue;
						} else {
							int aim = 0, mindeg = Integer.MAX_VALUE;
							while (cand.size() + level > MaxOne+1 && cand.size() > 0) {
								Map.Entry<Integer, HashSet<Integer>> firstEntry = od2c
										.firstEntry();
								aim = firstEntry.getValue().iterator().next();//
								mindeg = firstEntry.getKey();
								HashMap<Integer, Integer> aimSet = updateMarkDeg(
										aim, mindeg, cand, d2c, od2c);
								HashSet<Integer> aimnotset = genInterSet(
										notset, aim);
								Status ss = new Status(aim, level + 1, aimSet,
										aimnotset, null, null);
								if (aimSet.size() <= sizeN)
									computeSmallGraph(ss, context);
								else {
									stack.add(ss);
								}
								notset.add(aim);
								if (judgeClique(d2c)) {
									if (cand.size() > 0) {
										Map.Entry<Integer, HashSet<Integer>> lastEntry = od2c
												.lastEntry();
										aim = lastEntry.getValue().iterator()
												.next();
										notset.retainAll(verEdge.get(aim));
									}
									if(level+cand.size()<=MaxOne){
										break;
									}
									if (allContained(cand, notset)) {
										break;
									} else {
										emitClique(result, level, cand, context);
										break;
									}
								}
							}
						}
						t2 = System.currentTimeMillis();
						tPhase += (t2 - t1);
						t1 = t2;
						if (tPhase > TimeThreshold) {
							break;
						}
					}
					boolean needtospillveredge = !stack.empty();
					while (!stack.empty()) {
						// 退出了栈还没空，说明时间到了还没计算完，把栈中的子图spill到磁盘
						spillToDisk(stack.pop(), rnew);
					}
					if (needtospillveredge) {
						rnew.write(((-2) + "\t" + 1 + "#" ).getBytes());
						String pas = parts.toString();
						rnew.write(pas.substring(1, pas.length()-1).getBytes());
						rnew.write(("#"+tmpKey+"#").getBytes());
						writeVerEdge(verEdge, rnew);
						rnew.write(("\n").getBytes());// rnew.write(("\n0\t0\n").getBytes());
					}
					if (tPhase > TimeThreshold) {
						break;
					}// 超时之后，后面文件也不读了
				}// while
				while ((line = raf.readLine()) != null) {
					// 把原文件后面的内容直接考到新文件后面
					rnew.write(line.getBytes());
					rnew.write("\n".getBytes());
				}
				rnew.close();
			}
			raf.close();
			super.cleanup(context);
		}

		private void writeVerEdge(HashMap<Integer, HashSet<Integer>> edge,
				RandomAccessFile rf) throws IOException {
			for (Map.Entry<Integer, HashSet<Integer>> en : edge.entrySet()) {
				// rf.write(en.getKey());
				rf.write((en.getKey() + "=").getBytes());
				Iterator<Integer> it = en.getValue().iterator();
				rf.write(it.next().toString().getBytes());
				while (it.hasNext()) {
					rf.write(("," + it.next()).getBytes());
				}
				rf.write(" ".getBytes());
			}
		}

		private void readVerEdge(String s,
				HashMap<Integer, HashSet<Integer>> edge) {
			String[] items = s.split(" ");
			for (String item : items) {
				String[] abs = item.split("=");
				int key = Integer.parseInt(abs[0]);
				String[] values = abs[1].split(",");
				HashSet<Integer> adjs = new HashSet<Integer>();
				for (String value : values)
					adjs.add(Integer.parseInt(value));
				edge.put(key, adjs);
			}
		}

		protected void reduce(PairTypeInt key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			tmpKey = key.getC();
			int type = key.getA();
			vertex.clear();
			verEdge.clear();
			count = 0;
			notset.clear();
			cand.clear();
			deg2cand.clear();
			od2.clear();
			parts.clear();
			number = 0;
			cutNumber = 0;
			cutNumberAfter = 0;
			balanceOrNot = false;
			if (tPhase < TimeThreshold) {
				for (Text t : values) {
					// 读入一个完整的子图集合
					String ver = t.toString();

					if (type == 1) {// 是没有邻接表的不完整子图，或者是邻接表
						if (ver.contains("%")) {
							// 是不全的子图
							stack.add(new Status(ver));
						} else {
							// 是邻接表
							readVerEdge(ver, verEdge);
						}
					} else {// 是一个完整的包括了邻接表的，没有切分过的size-1子图
						String[] sts = ver.split("@");
						stack.add(new Status(sts[0]));
						readVerEdge(sts[1], verEdge);
					}
				}
			} else {
				// 边邻接信息写在最后
				String edgestr = "";
				for (Text t : values) {
					System.out.println("out"+t.toString());
					NodeSN++;
					String ver = t.toString();
					if (type == 1) {
						if (ver.contains("%")) {
							raf.write(("-1\t1@" + NodeSN % totalPart + "@"
									+ tmpKey + "@").getBytes());
							raf.write(ver.getBytes());
							raf.write("\n".getBytes());
						} else {
							edgestr = ver;
						}
					} else {
						raf.write(("-1\t0@" + NodeSN % totalPart + "@" + tmpKey + "@")
								.getBytes());
						raf.write(ver.getBytes());
						raf.write("\n".getBytes());
					}
				}
				raf.write(((-2) + "\t" + 1 + "#" +key.getB()).getBytes());
				raf.write(("#"+tmpKey+"#").getBytes());
				raf.write(edgestr.getBytes());
				raf.write("\n".getBytes());
				return;
			}
			long t1 = System.currentTimeMillis();
			long t2 = System.currentTimeMillis();
			while (!stack.empty()) {
				if (tPhase < TimeThreshold) // 时间小于阈值
				{
					Status top = stack.pop();
					HashSet<Integer> notset = top.getNotset();
					HashMap<Integer, Integer> cand = top.getCandidate();
					result = top.getResult();
					HashMap<Integer, HashSet<Integer>> d2c;
					TreeMap<Integer, HashSet<Integer>> od2c;
					int level = top.getLevel();
					int vp = top.getVp();
					if(level+cand.size()<=MaxOne){
						continue;
					}
					if (allContained(cand, notset)) {
						return;
					}
					if (result.size() + 1 == level) {
						result.add(vp);
					} else {
						result.set(level - 1, vp);
					}
					d2c = top.getDeg2cand();
					od2c = top.getOd2c();
					if (d2c == null) {
						d2c = new HashMap<Integer, HashSet<Integer>>();
						od2c = new TreeMap<Integer, HashSet<Integer>>();
						updateDeg(cand, d2c, od2c);
					}
					if (judgeClique(d2c)) {
						emitClique(result, level, cand, context);
						return;
					} else {
						int aim = 0, mindeg = Integer.MAX_VALUE;
						while (cand.size() + level > MaxOne+1 && cand.size() > 0) {
							Map.Entry<Integer, HashSet<Integer>> firstEntry = od2c
									.firstEntry();
							aim = firstEntry.getValue().iterator().next();//
							mindeg = firstEntry.getKey();
							HashMap<Integer, Integer> aimSet = updateMarkDeg(
									aim, mindeg, cand, d2c, od2c);
							HashSet<Integer> aimnotset = genInterSet(notset,
									aim);
							Status ss = new Status(aim, level + 1, aimSet,
									aimnotset, null, null);
							if (aimSet.size() <= sizeN)
								computeSmallGraph(ss, context);
							else {
								spillToDisk(ss, raf);
							}
							notset.add(aim);
							if (judgeClique(d2c)) {
								if (cand.size() > 0) {
									Map.Entry<Integer, HashSet<Integer>> lastEntry = od2c
											.lastEntry();
									aim = lastEntry.getValue().iterator()
											.next();
									notset.retainAll(verEdge.get(aim));
								}
								if(level+cand.size()<=MaxOne){
									break;
								}
								if (allContained(cand, notset)) {
									break;
								} else {
									emitClique(result, level, cand, context);
									break;
								}
							}
						}
					}
					t2 = System.currentTimeMillis();
					tPhase += (t2 - t1);
					t1 = t2;// 重新设定累计起点
				} else// 时间超阈值，状态直接发往下一个phase
				{
					break;
				}// 计算末尾
			}
			while (!stack.empty()) {// 未算完的spill到磁盘
				spillToDisk(stack.pop(), raf);
			}
			if (balanceOrNot) {
				// 若份数比totalPart大则每个reduce发一份，否则只发对应份
				// 记录最大，每个计算节点发一份
				raf.write(((-2) + "\t" + 1 + "#" ).getBytes());
				String pas = parts.toString();
				raf.write(pas.substring(1, pas.length()-1).getBytes());
				raf.write(("#"+tmpKey+"#").getBytes());
				writeVerEdge(verEdge, raf);
				raf.write(("\n").getBytes());
				// raf.write(("\n0\t0\n").getBytes());
			}

		}// reduce

		private void computeSmallGraph(Status ss, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Stack<Status> smallStack = new Stack<Status>();
			smallStack.add(ss);
			while (!smallStack.empty()) {
				Status s = smallStack.pop();
				HashSet<Integer> notset = s.getNotset();
				HashMap<Integer, Integer> cand = s.getCandidate();
				int vp = s.getVp(), level = s.getLevel();
				if(level+cand.size()<=MaxOne){
					continue;
				}
				if (allContained(cand, notset)) {
					continue;
				}
				if (result.size() + 1 == level) {
					result.add(vp);
				} else {
					result.set(level - 1, vp);
				}
				if (cand.isEmpty()) {
					if (notset.isEmpty()) {
						emitClique(result, level, cand, context);
					}
					continue;
				}
				int fixp = findMaxDegreePoint(cand);
				// int maxdeg = cand.get(fixp);
				if (isClique) {
					emitClique(result, level, cand, context);
					continue;
				}
				ArrayList<Integer> noneFixp = new ArrayList<Integer>(
						cand.size() - maxdeg);
				HashMap<Integer, Integer> tmpcand = genInterSet(cand, fixp,
						maxdeg, noneFixp);// �����Ѿ���fixp��cand����ɾ����
				HashSet<Integer> tmpnot = genInterSet(notset, fixp);
				Status tmp = new Status(fixp, level + 1, tmpcand, tmpnot, null,
						null);
				smallStack.add(tmp);
				// cand.remove(fixp);
				notset.add(fixp);
				for (int fix : noneFixp) {
					HashMap<Integer, Integer> tcand = genInterSet(cand, fix);// �����Ѿ���fixp��cand����ɾ����
					HashSet<Integer> tnot = genInterSet(notset, fix);
					Status temp = new Status(fix, level + 1, tcand, tnot, null,
							null);
					smallStack.add(temp);
					notset.add(fix);
				}
			}
		}

		private int findMaxDegreePoint(HashMap<Integer, Integer> cand) {
			int maxpoint = 0, tmpdeg = 0, size = cand.size() - 1;
			maxdeg = -1;
			isClique = true;

			for (Map.Entry<Integer, Integer> en : cand.entrySet()) {
				HashSet<Integer> adj = verEdge.get(en.getKey());
				tmpdeg = computeDeg(adj, cand.keySet());
				en.setValue(tmpdeg);
				if (tmpdeg > maxdeg) {
					maxdeg = tmpdeg;
					maxpoint = en.getKey();
				}
				if (isClique && tmpdeg != size) {
					isClique = false;
				}
			}
			return maxpoint;
		}

		private int computeDeg(HashSet<Integer> adj, Set<Integer> keySet) {
			int deg = 0;
			if (adj.size() > keySet.size()) {
				for (int k : keySet) {
					if (adj.contains(k))
						deg++;
				}
			} else {
				for (int k : adj) {
					if (keySet.contains(k))
						deg++;
				}
			}
			return deg;
		}

		private void spillToDisk(Status ss, RandomAccessFile raf)
				throws IOException {
			// TODO Auto-generated method stub
			balanceOrNot = true;
			parts.add(count%totalPart);
			StringBuilder sb = new StringBuilder();
			int p = (count % totalPart)%numReducer;
			parts.add(p);
			sb.append("-1\t1@");
			sb.append(p);
			sb.append("@" + tmpKey + "@");
			sb.append(ss.toString(result));
			/**
			sb.append("@");

			for (int i = 0; i < ss.getLevel() - 1; i++) {
				sb.append(result.get(i));
				sb.append(",");
			}*/
			sb.append("\n");
			count++;
			raf.write(sb.toString().getBytes());
		}

		private HashMap<Integer, Integer> updateMarkDeg(int aim, int mindeg,
				HashMap<Integer, Integer> cand,
				HashMap<Integer, HashSet<Integer>> d2c,
				TreeMap<Integer, HashSet<Integer>> od2c) {
			cand.remove(aim);
			HashSet<Integer> li = d2c.get(mindeg);
			li.remove(aim);
			if (li.isEmpty()) {
				d2c.remove(mindeg);
				od2c.remove(mindeg);
			}
			HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
			int acc = 0;
			HashSet<Integer> adj = verEdge.get(aim);
			if (adj.size() > cand.size()) {
				Iterator<Map.Entry<Integer, Integer>> it = cand.entrySet()
						.iterator();
				while (it.hasNext() && acc < mindeg) {
					Map.Entry<Integer, Integer> en = it.next();
					if (adj.contains(en.getKey())) {
						int point = en.getKey(), deg = en.getValue();
						HashSet<Integer> lis = d2c.get(deg);
						lis.remove(point);
						if (lis.isEmpty()) {
							d2c.remove(deg);
							od2c.remove(deg);
						}
						deg--;
						HashSet<Integer> list = d2c.get(deg);
						if (list == null) {
							list = new HashSet<Integer>();
							d2c.put(deg, list);
							od2c.put(deg, list);
						}
						list.add(point);
						en.setValue(deg);
						acc++;
						result.put(point, 0);
					}
				}
			} else {
				Iterator<Integer> it = adj.iterator();
				while (it.hasNext() && acc < mindeg) {
					int point = it.next();
					// Map.Entry<Integer, Integer> en = cand.
					if (cand.containsKey(point)) {
						int deg = cand.get(point);
						HashSet<Integer> lis = d2c.get(deg);
						lis.remove(point);
						if (lis.isEmpty()) {
							d2c.remove(deg);
							od2c.remove(deg);
						}
						deg--;
						HashSet<Integer> list = d2c.get(deg);
						if (list == null) {
							list = new HashSet<Integer>();
							d2c.put(deg, list);
							od2c.put(deg, list);
						}
						list.add(point);
						cand.put(point, deg);
						acc++;
						result.put(point, 0);
					}
				}
			}
			return result;
		}

		private void emitClique(ArrayList<Integer> result2, int level,
				HashMap<Integer, Integer> cand, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			numClique++;
			for (int i = 1; i < level; i++) {
				sb.append(result.get(i)).append(" ");
			}
			for (int i : cand.keySet()) {
				sb.append(i).append(" ");
			}
			context.write(new IntWritable(result.get(0)),
					new Text(sb.toString()));
		}

		private boolean judgeClique(HashMap<Integer, HashSet<Integer>> d2c) {
			// TODO Auto-generated method stub
			if (d2c.size() > 1)
				return false;
			if (d2c.size() == 0)
				return true;
			if (d2c.size() == 1) {
				Map.Entry<Integer, HashSet<Integer>> first = d2c.entrySet()
						.iterator().next();
				if (first.getKey() + 1 == first.getValue().size())
					return true;
			}
			return false;
		}

		private void updateDeg(HashMap<Integer, Integer> cand,
				HashMap<Integer, HashSet<Integer>> d2c,
				TreeMap<Integer, HashSet<Integer>> od2c) {
			// TODO Auto-generated method stub
			System.out.println("veredge:"+verEdge);
			int deg = 0;
			HashSet<Integer> adj;
			for (Map.Entry<Integer, Integer> en : cand.entrySet()) {
				int cad = en.getKey();
				adj = verEdge.get(cad);
				deg = 0;
				if (adj.size() > cand.size()) {
					for (int k : cand.keySet()) {
						if (adj.contains(k)) {
							deg++;
						}
					}
				} else {
					for (int k : adj) {
						if (cand.containsKey(k)) {
							deg++;
						}
					}
				}
				HashSet<Integer> d2cset = d2c.get(deg);
				if (d2cset == null) {
					d2cset = new HashSet<Integer>();
					d2c.put(deg, d2cset);
					od2c.put(deg, d2cset);
				}
				d2cset.add(cad);
				en.setValue(deg);
			}
		}

		private HashMap<Integer, Integer> genInterSet(
				HashMap<Integer, Integer> cand, int aim, int maxdeg,
				ArrayList<Integer> noneFixp) {
			HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
			int acc = 0;
			HashSet<Integer> adj = verEdge.get(aim);
			cand.remove(aim);
			Iterator<Integer> it = cand.keySet().iterator();
			int tmp;
			while (acc < maxdeg && it.hasNext()) {
				tmp = it.next();
				if (adj.contains(tmp)) {
					result.put(tmp, 0);
				} else {
					noneFixp.add(tmp);
				}
			}
			while (it.hasNext())
				noneFixp.add(it.next());

			/**
			 * HashSet<Integer> small ,big; if(adj.size()>cand.size()){ small =
			 * (HashSet<Integer>) cand.keySet(); big = adj; }else{ big =
			 * (HashSet<Integer>) cand.keySet(); small = adj; }
			 * Iterator<Integer> it = small.iterator(); int tmp; while(acc <
			 * maxdeg && it.hasNext()){ tmp = it.next(); if(big.contains(tmp)){
			 * acc++; result.put(tmp, 0); } }
			 */
			return result;
		}

		private HashMap<Integer, Integer> genInterSet(
				HashMap<Integer, Integer> cand, int aim) {
			HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
			int acc = 0;
			HashSet<Integer> adj = verEdge.get(aim);
			cand.remove(aim);
			Set<Integer> small, big;
			if (adj.size() > cand.size()) {
				small = cand.keySet();
				big = adj;
			} else {
				big = cand.keySet();
				small = adj;
			}
			Iterator<Integer> it = small.iterator();
			int tmp;
			while (it.hasNext()) {
				tmp = it.next();
				if (big.contains(tmp)) {
					acc++;
					result.put(tmp, 0);
				}
			}
			return result;
		}

		private HashSet<Integer> genInterSet(HashSet<Integer> notset, int aim) {
			HashSet<Integer> result = new HashSet<Integer>();
			HashSet<Integer> adj = verEdge.get(aim);
			if (adj.size() > notset.size()) {
				for (int i : notset) {
					if (adj.contains(i))
						result.add(i);
				}
			} else {
				for (int i : adj) {
					if (notset.contains(i))
						result.add(i);
				}
			}
			return result;
		}

		private boolean allContained(HashMap<Integer, Integer> cand,
				HashSet<Integer> notset) {
			for (int nt : notset) {
				HashSet<Integer> nadj = verEdge.get(nt);
				if (nadj.containsAll(cand.keySet()))
					return true;
			}
			return false;
		}

	}// reducer

}// class
