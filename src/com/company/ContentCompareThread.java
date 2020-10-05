package com.company;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.*;

public class ContentCompareThread extends Thread {
	List<List<String>> matches = new ArrayList<>();
	private HashMap<String,byte[]> contents = new HashMap<>();

	public ContentCompareThread(ThreadGroup compareGroup) {
		super(compareGroup,"Comparator");
	}

	@Override
	public void run() {
		List<String> sameSize;
		main: while((sameSize = Main.getNextCompareFiles()) != null) {
			if(sameSize.size() == 1) {
				Main.checkedFiles++;
				continue;
			}
			Map<String, FileInputStream> streams = new HashMap<>(sameSize.size());
			for (int i = 0; i < sameSize.size(); i++) {
				try {
					streams.put(sameSize.get(i), new FileInputStream(sameSize.get(i)));
				} catch (FileNotFoundException e) {
					Main.exceptions.put(sameSize.get(i), e);
				}
			}
			if(streams.size() == 1)
				Main.checkedFiles++;
			if(streams.size() <= 1)
				continue main;
			int buffSize = Main.buffer;
			boolean eof = false;
			while(!eof) {
				contents.clear();
				if(streams.size() == 1)
					Main.checkedFiles++;
				if(streams.size() <= 1)
					continue main;
				List<String> toremove = new ArrayList();
				for (Map.Entry<String, FileInputStream> file : streams.entrySet()) {
					byte[] buff = new byte[buffSize];
					try {
						eof = file.getValue().read(buff) < buffSize;
					} catch (IOException e) {
						Main.checkedFiles++;
						Main.exceptions.put(file.getKey(), e);
						toremove.add(file.getKey());
						contents.remove(file);
						if(streams.size() == 1)
							Main.checkedFiles++;
						if(streams.size() <= 1)
							continue main;
					}
					contents.put(file.getKey(), buff);
				}
				for (String s : toremove) {
					streams.remove(s);
				}

				List<String> removed;
				synchronized (matches) {
					removed = processContent(matches);
					contents.clear();
					if (matches.size() == 0) {
						Main.checkedFiles += sameSize.size();
						continue main;
					}
				}
				for (String file : removed) {
					streams.remove(file);
					if(streams.size() == 1)
						Main.checkedFiles++;
					if(streams.size() <= 1)
						continue main;
				}
			}
			Main.checkedFiles += sameSize.size();
		}
	}

	private List<String> processContent(List<List<String>> result) {
		Collection<byte[]> halfUnique = contents.values();
		Set<byte[]> unique = new HashSet<>();
		for (byte[] bytes : halfUnique) {
			if(unique.stream().noneMatch(arr -> Arrays.equals(arr, bytes))){
				unique.add(bytes);
			}
		}
		List<List<String>> localResult = new ArrayList<>();
		List<List<String>> oldResult = new ArrayList<>();
		oldResult.addAll(result);
		List<String> removed = new ArrayList<>();
		for (byte[] cont : unique){
			List<String> same = contents.entrySet().stream().filter(val -> Arrays.equals(cont, val.getValue())).map(e -> e.getKey()).collect(toList());
			if(same.size() == 1) {
				contents.remove(same.get(0));
				removed.add(same.get(0));
			}
			else {
				localResult.add(same);
			}
		}
		if(result.size() == 0){
			result.addAll(localResult);
		}else{
			result.clear();
			for (List<String> oldSet : oldResult) {
				for (List<String> newSet : localResult) {
					List<String> crossSection = oldSet.stream().filter(e->newSet.contains(e)).collect(toList());
					if(crossSection.size() > 1)
						result.add(crossSection);
				}
			}
		}

		return removed;
	}
}
