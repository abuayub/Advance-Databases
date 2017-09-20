/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package graphfrequencyunconstrained;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Soumyava
 */

public class GraphFrequencyUnconstrained {

    /**
     * @param args the command line arguments
     */
    static int counter = 0;
    Map<String,Collection<String>> adjList; //This stores the adjacency list
    Set<String> expandedEdges; // This stores the current instances to be expanded
    Set<String> nextLevelEdges; //This stores the expanded 
    Map<String, ArrayList<String>> HashPrune;
    Map<String, Map<String,Integer>> CollectionVertices;
    Map<Double,ArrayList<String>> beamMap;
    Set<String> frequentSubs;
    ArrayList<String> subs;
    static boolean DEBUG_FLAG = false;
    static boolean CHECK_OVERLAP = true;
    static int noOfIterations = 8;
    static int noOfPartitions = 8;
    static int minfrequents= 1000;
    //static String inputFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\processed_T100KV200KE_embed_5_vl_100_el_200.g";
    static String inputFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\processed_T100KV800KE_embed_10_vl_100_el_800.g";
    static String randomInputFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Rprocessed_T100KV800KE_embed_10_vl_100_el_800.g";
    static String adjlistFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\AdjListFull.txt";
    //static String inputFileName = "C:\\Users\\Soumyava\\Desktop\\datasets\\optimization testcases\\new_expts_feb_2\\processed_T100KV200KE_12_vl_12_el.g";
    //static String inputFileName =  "C:\\Users\\Soumyava\\Desktop\\datasets\\test_code\\processed_Das10KV20KE_10VL_20EL.g";
    //static String inputFileName = "C:\\Users\\Soumyava\\Desktop\\datasets\\optimization testcases\\new_expts_feb_2\\processed_T100KV200KE_10000_vl_10000_el.g";
    //static int beam=4;
    
    public Set<String> getEdges(){
        return expandedEdges;
    }
    
    private void readConstraintsFile() throws FileNotFoundException, IOException{
        
    }  
    
    public static void RandomizeFile(String input,String output){
    	ArrayList<String> lines = new ArrayList<>();
    	try( BufferedReader br = new BufferedReader(new FileReader(input))){
    		String str = "";    		
    		while((str=br.readLine()) !=null ){    			 
    			 lines.add(str);
    		}
    	}catch(Exception ex){
    		System.out.println(ex);
    	}
    	Collections.shuffle(lines);
    	try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(output), "utf-8"))) {
    		for(String line:lines){
    			writer.write(line);
    			writer.write("\n");
    		}
    		
    	}catch(Exception ex){
    		System.out.println(ex);
    	}
    }
    
    public static void LoadAdjList(HashSet<String> S, int iter, int partition){
    	String iterationOutputAdjFilename = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\AL_"+(partition+1)+"_"+(iter+1)+".txt";
    	String IterationFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\AL_"+(partition+1)+"_"+iter+".txt";
    	try{
    		Files.copy(Paths.get(IterationFileName), Paths.get(iterationOutputAdjFilename),StandardCopyOption.REPLACE_EXISTING);
    	}catch(Exception ex){
    		System.out.println(ex);
    	}
    	HashSet<String> pin = new HashSet<>();
    	HashSet<String > vin = new HashSet<>();
    	HashSet<String > vout = new HashSet<>();
    	HashSet<String > redundant = new HashSet<>();
    	try( BufferedReader br = new BufferedReader(new FileReader(IterationFileName))){
    		String str = br.readLine();    		
    		while(str !=null ){    			 
    			 String id = str.split(":")[0];
    			 pin.add(id);
    			 str = br.readLine();
    		}
    		int Ssz = S.size();
    		int pinsz = pin.size();
    		vin = new HashSet<>(pin);
    		redundant = new HashSet<>(pin);
    		
    		vout = new HashSet<>(S);
    		vin.retainAll(S);
    		int vinsz = vin.size();
    		redundant.removeAll(vin);
    		vout.removeAll(vin);
    		
    		
    	}catch(Exception ex){
    		System.out.println(ex);
    	}
    	
    	HashSet<String> remainingIds = new HashSet<>(vout);
    	HashSet<String> commonIds = new HashSet<>(vout);
    	String partitioncontribution = ""+(iter+1);
    	String partitionHeader = "Iteration No";
    	try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(iterationOutputAdjFilename,true), "utf-8"))) {
			for(int i = 0;i<noOfPartitions ;i++){   
				if(i==partition)
					continue;
				partitionHeader+=",Cumulative Partition"+(i+1)+",Individual Contribution"+(i+1);
				String otherPartitionFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\AL_"+(i+1)+"_"+iter+".txt";
				int individualContribution = 0;
				int cumulativeContribution = 0;
				try( BufferedReader br1 = new BufferedReader(new FileReader(otherPartitionFileName))){
					String str=br1.readLine();
		    		//HashMap<String,String> adjacencyMap = new HashMap<>();
		    		HashSet<String> availableIds = new HashSet<>();
		    		while(str !=null ){
		    			 
		    			 String[] keyVal = str.split(":");
		    			 availableIds.add(keyVal[0]);
		    			 
		    			 if(vout.contains((keyVal)[0])){
		    				 individualContribution++;		    				 
		    			 }
		    			 
		    			 if(remainingIds.contains(keyVal[0])){
		    				 cumulativeContribution++;
		    				 remainingIds.remove(keyVal[0]);
		    				 writer.write(str);
		    			 }
		    			 str = br1.readLine();
		    	            //String vId = keyVal[1];
		    			 //adjacencyMap.put(keyVal[0], keyVal[1]);  	    	            
		    	         //availableIds.add(keyVal[0]);
		    		}
		    		commonIds.retainAll(availableIds);
		    		if(vout.size()>0){
			    		System.out.printf("Sequential Contribution of Partition %d for expanding Partition %d in iteration %d is %f of %d vertex . \n",i,partition,iter,(float)((float)cumulativeContribution/(float)vout.size())*100,cumulativeContribution);
			    		System.out.printf("Individual Contribution of Partition %d for expanding Partition %d in iteration %d is %f of %d vertex . \n",i,partition,iter,(float)((float)individualContribution/(float)vout.size())*100,individualContribution);
		    		}else{
		    			//System.out.printf("Contribution for expanding Partition %d in iteration %d is not required. All vid present  . \n",partition,iter);
//			    		System.out.printf("Individual Contribution of Partition %d for expanding Partition %d in iteration %d is %d  . \n",i,partition,iter,0);
		    			
		    		
		    		}
		    		if(vout.size()>0){
		        		partitioncontribution+=",";
		        		partitioncontribution+=(float)((float)cumulativeContribution/(float)vout.size())*100;
		        		partitioncontribution+=",";
		        		partitioncontribution+=(float)((float)individualContribution/(float)vout.size())*100;
		    		}else{
		    			partitioncontribution+=",0";		        		
		        		partitioncontribution+=",0";
		        		
		    		}
		    		
		    		
				}catch(Exception ex){
					System.out.println(ex);
				}
			}
    		String IterationpartitionChartFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\Chart_P"+(partition+1)+".csv";

	    	try (Writer writer1 = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(IterationpartitionChartFileName,true), "utf-8"))) {
	    		if(iter ==0){
	    			writer1.append(partitionHeader);
	    			writer1.append("\n");
	    		}
      		writer1.append(partitioncontribution);
      		writer1.append("\n");
	    	}

			System.out.printf("No of common vertex in all partitions to expand Partition %d in iteration %d is %d vertex . \n",partition,iter,commonIds.size());
    		
    	}catch(Exception ex){
    		System.out.println(ex);
    	}
    	
    }
    
    
    public static void IterationPartition(int iter, int noOfLines){
        	String IterationFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\I_"+(iter+1)+".txt";
        	try{ BufferedReader br = new BufferedReader(new FileReader(IterationFileName));
        	int noOfLinesLeft = noOfLines;
        	for(int i = 0;i<noOfPartitions;i++){
        		int noOfLinesPerPartition = (int)Math.ceil(noOfLinesLeft/(noOfPartitions-i));
        		noOfLinesLeft -=noOfLinesPerPartition;
	        	String IterationpartitionFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\P_"+(iter+1)+"_"+(i+1)+".txt";
	        	try (Writer writer = new BufferedWriter(new OutputStreamWriter(
	                    new FileOutputStream(IterationpartitionFileName), "utf-8"))) {
	        		String e1="";
	        		HashSet<String> uniqueElemets= new HashSet<>(); 
	        		for(int j=0;j<noOfLinesPerPartition&&(e1=br.readLine())!=null;j++){
	        			//e1 = br.readLine();
	        			writer.write(e1);
	        			writer.write("\n");
	        			
	        			// Finding unique elements in a partition
	        			String[] elements = e1.split(":");
	        			elements=elements[1].split(",");
	                    for(int m=1;m<elements.length;m=m+2){
	                    	uniqueElemets.add(elements[m]);
		        			
	                    }
	        		}
	        		// Compare Vin and Vout
	        		LoadAdjList(uniqueElemets,iter,i );
	        		}
        		}        	br.close();

	        }catch(Exception ex){
	        	int a = 8;
	        }
    	}
    	

    public static void InitialAdjFull(){
    	try{
    	Map<String,Collection<String>> adjacencyMap;
        adjacencyMap = new HashMap<String,Collection<String>>();
        BufferedReader br = new BufferedReader(new FileReader(inputFileName));
        String[] keyVal;
        String str = br.readLine();
        while(str!=null){
            keyVal = str.split(",");
            String vId = keyVal[1];
            if(adjacencyMap.containsKey(keyVal[1])){
            	adjacencyMap.get(keyVal[1]).add(str);
            }
            if(adjacencyMap.containsKey(keyVal[3])){
            	adjacencyMap.get(keyVal[3]).add(str);
            }
            if(!adjacencyMap.containsKey(keyVal[1])){
                ArrayList<String> edges1 = new ArrayList<String>();
                edges1.add(str);
                adjacencyMap.put(keyVal[1], edges1);
            }
            if(!adjacencyMap.containsKey(keyVal[3])){
                ArrayList<String> edges1 = new ArrayList<String>();
                edges1.add(str);
                adjacencyMap.put(keyVal[3], edges1);
            }
            str = br.readLine();
            
        }
        br.close();
    	try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(adjlistFileName), "utf-8"))) {
            for(String k: adjacencyMap.keySet()){
            	writer.write(k);
            	writer.write(":");
            	writer.write(String.join(";", adjacencyMap.get(k)));
            	writer.write("\n");
            }
    	}
    	String[] filenameSplit = inputFileName.split("\\\\");
    	String fileNamePart = filenameSplit[filenameSplit.length-1];
    	String edgeNumbers = fileNamePart.split("_")[7];
    	edgeNumbers = edgeNumbers.substring(0, edgeNumbers.length()-2);
    	int noOfEdge = Integer.parseInt(edgeNumbers)*1000;
    	int nooflines = (int) Math.ceil(noOfEdge/noOfPartitions);
		BufferedReader br1 = new BufferedReader(new FileReader(inputFileName));
    	for(int iter = 0;iter<noOfPartitions;iter++){
    		HashSet<String> s= new HashSet<>(); 
        	String partitionFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\P_"+(iter+1)+".txt";
        	String adjlistFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\AL_"+(iter+1)+"_0"+".txt";
        	try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(partitionFileName), "utf-8"))) {
        		try (Writer writer1 = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(adjlistFileName), "utf-8"))) {
        			String e1="";
            	for(int i = iter*nooflines;i<(iter+1)*nooflines && (e1=br1.readLine())!=null;i++){
                    //String e1 = br1.readLine();
            		writer.write(e1);
            		writer.write("\n");
            		
            		//Create adj for P[iter]
            		String[] edgeKey = e1.split(",");
            		if(!s.contains(edgeKey[1])){
            			s.add(edgeKey[1]);
            			writer1.write(edgeKey[1]);
            			writer1.write(":");
            			writer1.write(String.join(";", adjacencyMap.get(edgeKey[1])));
            			writer1.write("\n");
            		}
            		if(!s.contains(edgeKey[3])){
            			s.add(edgeKey[3]);
            			writer1.write(edgeKey[3]);
            			writer1.write(":");
            			writer1.write(String.join(";", adjacencyMap.get(edgeKey[3])));
            			writer1.write("\n");
            		}
            		
            	}
        	}
        }
    	}
    	br1.close();
    	}catch(Exception ex){
    		System.out.println(ex);
    	}

    }
    public GraphFrequencyUnconstrained(){
        adjList = new HashMap<String, Collection<String>>();
        expandedEdges = new HashSet<String>();
        nextLevelEdges = new HashSet<String>();
        HashPrune=new HashMap<String, ArrayList<String>>();
        CollectionVertices=new HashMap<String, Map<String,Integer>>();
        beamMap = new HashMap<Double,ArrayList<String>>();
        subs = new ArrayList<String>();
        frequentSubs = new HashSet<String>();
        //inputFileName = "";
        //readConstraintsFile();
    }
    
    private int populateAdjList(String fileName, int threshold) throws FileNotFoundException, IOException{
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String[] keyVal;
        String key;
        ArrayList<String> edges = new ArrayList<>();
        int edgecount=0;
        Map<String,ArrayList<String>> freqs = new HashMap<>();
        Set<String> vSet = new HashSet<String>();
        Set<String> eSet = new HashSet<String>();
        try{
            String str = br.readLine();
            while(str != null){
            keyVal = str.split(",");
            key = keyVal[0]+","+keyVal[2]+","+keyVal[4]+",1,2";
            if (freqs.containsKey(key)){
                freqs.get(key).add(str);
            }
            else{
                ArrayList<String> al = new ArrayList<>();
                al.add(str);
                freqs.put(key, al);
            }
            str = br.readLine();
            }
        }catch(Exception e){
            
        }finally{
            for(String k: freqs.keySet()){
                if (freqs.get(k).size()>=threshold){
                    edges.addAll(freqs.get(k));
                }
            }
        }
        for(String e: edges){
            keyVal = e.split(",");
            vSet.add(keyVal[2]);
                vSet.add(keyVal[4]);
                eSet.add(keyVal[0]);
                if(adjList.containsKey(keyVal[1])){
                    adjList.get(keyVal[1]).add(e);
                }
                if(adjList.containsKey(keyVal[3])){
                    adjList.get(keyVal[3]).add(e);
                }
                if(!adjList.containsKey(keyVal[1])){
                    ArrayList<String> edges1 = new ArrayList<String>();
                    edges1.add(e);
                    adjList.put(keyVal[1], edges1);
                }
                if(!adjList.containsKey(keyVal[3])){
                    ArrayList<String> edges1 = new ArrayList<String>();
                    edges1.add(e);
                    adjList.put(keyVal[3], edges1);
                }
                ArrayList<Integer> part = new ArrayList<Integer>();
                for(int i=0;i<5;i++){
                    part.add(0);
                }
                //String src = keyVal[2];
                //String dest = keyVal[4];
                //String str1 = keyVal[0]+"@"+str;
                expandedEdges.add(e);
        }
        return 0;
    }
    
    private int populateAdjList(String fileName) throws FileNotFoundException, IOException{
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String[] keyVal;
        int edgecount=0;
        Set<String> vSet = new HashSet<String>();
        Set<String> eSet = new HashSet<String>();
        try{
            String str = br.readLine();
            while(str!=null){
                edgecount++;
            	keyVal = str.split(",");
                vSet.add(keyVal[2]);
                vSet.add(keyVal[4]);
                eSet.add(keyVal[0]);
                if(adjList.containsKey(keyVal[1])){
                    adjList.get(keyVal[1]).add(str);
                }
                if(adjList.containsKey(keyVal[3])){
                    adjList.get(keyVal[3]).add(str);
                }
                if(!adjList.containsKey(keyVal[1])){
                    ArrayList<String> edges = new ArrayList<String>();
                    edges.add(str);
                    adjList.put(keyVal[1], edges);
                }
                if(!adjList.containsKey(keyVal[3])){
                    ArrayList<String> edges = new ArrayList<String>();
                    edges.add(str);
                    adjList.put(keyVal[3], edges);
                }
                ArrayList<Integer> part = new ArrayList<Integer>();
                for(int i=0;i<5;i++){
                    part.add(0);
                }
                //String src = keyVal[2];
                //String dest = keyVal[4];
                //String str1 = keyVal[0]+"@"+str;
                expandedEdges.add(str);
                str = br.readLine();
            }
        
        }
        
        catch(Exception e){
        }finally{
            br.close();
            
            if(DEBUG_FLAG)
            {
                System.out.println("Adj List is \n "+adjList);
                System.out.println("Size of adj list = "+adjList.size());
                System.out.println("Unique vertex labels = "+vSet.size());
                System.out.println("Unique edge labels = "+eSet.size());
            }
        }
        return edgecount;
    }
    
    private void expandEdges(String value,int iteration) {
        Set<String> visitedNodes = new HashSet<String>();
        Set<String> visitedEdges = new HashSet<String>();
        String[] strList;
        //substitute value at the k-edge instance you are expanding
        strList = value.split(",");
        for(int i=1;i<strList.length;i=i+2){
            visitedNodes.add(strList[i]);
        }
        for(String s : value.split(";")){
            visitedEdges.add(s);
        }
        if(DEBUG_FLAG)
            System.out.println("\nExpanding "+value);

        for(String v : visitedNodes){
            for(String s : adjList.get(v)){
                if(!visitedEdges.contains(s)){
                    if(DEBUG_FLAG)
                        System.out.println("Expanding on vertex "+v+" and adding edge "+s);
                    String toBeEmitted = getOrderedExpandedSub(value,s);
                    if(DEBUG_FLAG)
                        System.out.println("The expanded structure is "+toBeEmitted);
                    subs.add(toBeEmitted);
                    nextLevelEdges.add(toBeEmitted);
                    counter++;
                }
            }
        }
        
    }
    
    private String getOrderedExpandedSub(String value, String str) {
        //this function will insert str into value following lexicographic order
        String toBeEmitted = "";
        boolean insertFlag = false;
        for(String s: value.split(";")){
            //System.out.println("Striung prepared is "+toBeEmitted+" flag status = "+insertFlag);
            if(insertFlag == true){
                toBeEmitted = toBeEmitted + s + ";";
                continue;
            }
            int c = compareEdge(s,str);
            // if c=0 strings are equal and will not be a case
            // if c > 0 s is lexicographically more than str
            // c < 0 otherwise
            if(c<0){
                toBeEmitted = toBeEmitted + s +";";
            }
            else{
                toBeEmitted = toBeEmitted + str +";"+s+";";
                insertFlag = true;
            }
        }
        //all subs are checked
        //corner case the new edge to be placed at end
        if(insertFlag == false){
            toBeEmitted = toBeEmitted + str + ";";
        }
        return toBeEmitted;
    }
    
    private int compareEdge(String edgeInInstance, String edgeToBeAdded) {
            String eII = edgeInInstance.split(",")[0]+","+edgeInInstance.split(",")[2]+","
                    +edgeInInstance.split(",")[4]+","+edgeInInstance.split(",")[1]+","+edgeInInstance.split(",")[3];
            String eTA = edgeToBeAdded.split(",")[0]+","+edgeToBeAdded.split(",")[2]+","
                    +edgeToBeAdded.split(",")[4]+","+edgeToBeAdded.split(",")[1]+","+edgeToBeAdded.split(",")[3];
            //the value 0 if the argument string is equal to this string; 
            //a value less than 0 if this string is lexicographically less 
            //than the string argument; and a value greater than 0 if this 
            //string is lexicographically greater than the string argument.
            return eII.compareTo(eTA);
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException{
    	RandomizeFile(inputFileName, randomInputFileName);
    	inputFileName = randomInputFileName;
    	InitialAdjFull();
        GraphFrequencyUnconstrained ep = new GraphFrequencyUnconstrained();
        int noofedges = ep.populateAdjList(inputFileName,minfrequents);
        //int noofedges = ep.populateAdjList(inputFileName);
        int noofvertices=ep.adjList.size();
        System.out.println("Iteration,success,total,failure,dupSuccess,goodSuccess,expansion time,prune time, label generation time");
      
        //System.out.println("Iteration,duplicates,expansion time,prune time, label generation time");
        if (DEBUG_FLAG){
            System.out.println("Edges:"+noofedges);
            System.out.println("Vertices:"+noofvertices);
        }
        //iterate over the initial 1-edge list and expand substructures
        for(int iter = 0;iter<noOfIterations;iter++){
            //System.out.println("Iteration:"+iter);
        	String intermediateFileName = "C:\\_Ayub\\UTA MS CS\\Semester 2\\Adv DB\\Project\\Results\\I_"+(iter+1)+".txt";
        	try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(intermediateFileName), "utf-8"))) {
      
        	final long startTime = System.currentTimeMillis();
            counter = 0;
            if(DEBUG_FLAG)
                System.out.println("TOTAL SUBS CONSIDERED = "+ep.getEdges().size());
            double byt = 0;
            for(String s : ep.getEdges()){
                ep.expandEdges(s,iter);
                byt += s.getBytes().length;
            }
            byt = byt/ep.getEdges().size();
            //System.out.println("Size in bytes = "+byt);
             
            //System.out.println("Expansion finished");
            final long endTime = System.currentTimeMillis();
            //System.out.println("Expansion time = "+(endTime-startTime)/1000.0+" sec");
            // subs arraylist has been populated
            // pruning will require a pass over this list and then removing stuff
            ep.nextLevelEdges.clear();
            //System.out.println("Currently have "+ep.subs.size()+" subs to consider");
            final long pruneStart = System.currentTimeMillis();
            for (String  its : ep.subs){
                ep.nextLevelEdges.add(its);
            }
            //System.out.println("Total expanded subs are "+ep.nextLevelEdges.size());
            ep.subs.clear();
            //System.out.println("Currently have "+ep.subs.size()+" subs to consider");
            final long pruneEnd = System.currentTimeMillis();
            //System.out.println("Prune time = "+(pruneEnd-pruneStart)/1000.0+" sec");
            //System.out.println(ep.nextLevelEdges);
            //System.out.println("Unconstrained comparisons = "+counter);
            if(DEBUG_FLAG)
                System.out.println("counter = "+counter+" rest = "+ep.nextLevelEdges.size());
            int dups = counter-ep.nextLevelEdges.size();
            //System.out.println("Subs expanded = "+ep.nextLevelEdges.size()+ " duplicates = "+(counter-ep.nextLevelEdges.size()));
            ep.expandedEdges.clear();
            //System.out.println("Size = "+ep.nextLevelEdges.size());
            final long startTime1 = System.currentTimeMillis();
            Map<String,Integer> relMap = new HashMap<>();
            String[] edgeSplits;
            String[] edgeComps;
            //System.out.println("Key generation starts for "+ep.nextLevelEdges.size()+" subs");
            String hKey="";
            int cp = 0;
            //System.out.println(ep.nextLevelEdges);
            for(String s:ep.nextLevelEdges){
                cp++;
                //System.out.println(cp);
            	// s is the k-edge substructure we are trying to find the canonical label of
                // logic find the relative postition of vertices in the substructure
                int count = 1;
                edgeSplits = s.split(";");
                for (String e : edgeSplits){
                    edgeComps = e.split(",");
                    if (!relMap.containsKey(edgeComps[1])){
                        relMap.put(edgeComps[1], count);
                        count++;
                    }
                    if (!relMap.containsKey(edgeComps[3])){
                        relMap.put(edgeComps[3], count);
                        count++;
                    }
                    hKey = hKey+edgeComps[0]+","+edgeComps[2]+","+edgeComps[4]+","
                            +relMap.get(edgeComps[1])+","+relMap.get(edgeComps[3])+";";
                }
                if(ep.HashPrune.containsKey(hKey)){
                    boolean add = ep.HashPrune.get(hKey).add(s);
                }
                else{
                    ArrayList<String> ed = new ArrayList<String>();
                    ed.add(s);
                    ep.HashPrune.put(hKey,ed);
                }
                relMap.clear();
                hKey = "";
            }
            //System.out.println("Label generation completed");
            //System.out.println("Size of map = "+ep.HashPrune);
            ArrayList<String> minCalc = new ArrayList<String>();
            Map<String,Set<String>> vCount = new HashMap<String, Set<String>>();
            String[] vertexSplits;
            String[] keySplits;
            String[] keyInds;
            String[] vInds;
            int noOfKeys = 0;
            for (String k : ep.HashPrune.keySet()){
            	
                minCalc = ep.HashPrune.get(k);
                //System.out.println("Key = "+k+" value = "+minCalc );
                if (minCalc.size() < minfrequents)
                    continue;
                
                //System.out.println(k+" "+ep.HashPrune.get(k));
                keySplits = k.split(";");
                for (String key : keySplits){
                    keyInds = key.split(",");
                    vCount.put(keyInds[1], new HashSet<String>());
                    vCount.put(keyInds[2], new HashSet<String>());
                }
                
                //System.out.println(k);
                //System.out.println(minCalc);
                for(String key1 : minCalc){
                    vertexSplits = key1.split(";");
                    for(String key2:vertexSplits){
                        vInds = key2.split(",");
                        vCount.get(vInds[2]).add(vInds[1]);
                        vCount.get(vInds[4]).add(vInds[3]);
                    }
                }
                int min = 1000000;
                for(String das1: vCount.keySet()){
                    if(vCount.get(das1).size()<=min){
                        min = vCount.get(das1).size();
                    }
                }
                if (min >= minfrequents){
                	
                    ep.expandedEdges.addAll(minCalc);
                    //write to file
                    writer.write(k);
                	noOfKeys++;
                    writer.write(":");
                    writer.write(String.join("", minCalc));
                    writer.write("\n");
                }
                
                //System.out.println(vCount);
                //if (min >= minfrequents){
                    /*for(String eachSubg : minCalc){
                        ep.expandedEdges.add(k)
                    }*/
                //    ep.expandedEdges.addAll(minCalc);
                //}
                vCount.clear();
                
            }
            IterationPartition(iter, noOfKeys);
            //System.out.println("BM= "+ep.beamMap);
            //System.out.println("beam made");
            //for(Double d: ep.beamMap.keySet()){
            //    ep.expandedEdges.addAll(ep.beamMap.get(d));
            //}
            //ep.beamMap.clear(); 
            //System.out.println("finished frequency computation");
            //System.out.println("Carrying forward = "+ep.expandedEdges.size());
            //System.out.println(ep.expandedEdges);
            final long endTime1 = System.currentTimeMillis();
            //System.out.println("canonical labelling time:"+(endTime1 - startTime1)/1000.0);
            ep.HashPrune.clear();
            //System.out.println("The edges to be expanded are "+ep.expandedEdges);
            ep.nextLevelEdges.clear();
            //System.out.println("Edges to expand now = "+ep.expandedEdges.size()+" initial next level ="+ep.nextLevelEdges.size());
            //System.out.println(" Participation list = "+ep.participationList);
            System.out.println((iter+1)+","+counter+","+counter+",0,"+dups+","+(counter-dups)+","+(endTime-startTime)/1000.0+","+(pruneEnd-pruneStart)/1000.0+","+(endTime1-startTime1)/1000.0);
        
            //System.out.println(iter+","+dups+","+(endTime-startTime)/1000.0+","+(pruneEnd-pruneStart)/1000.0+","+(endTime1-startTime1)/1000.0);
        }catch(Exception ex){}
        }
    }

    
    
}
