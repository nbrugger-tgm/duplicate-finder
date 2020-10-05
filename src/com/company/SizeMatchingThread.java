package com.company;

import java.io.File;
import java.util.*;

public class SizeMatchingThread extends Thread{
	private final Set<Long> subset;
	List<List<String>> sameSized = new ArrayList<>();


	public SizeMatchingThread(ThreadGroup groupers, Set<Long> subset, int i) {
		super(groupers,"SizeMatcher"+ i);
		this.subset = subset;
	}

	@Override
	public void run() {
		for (Long l : subset){
			List<String> matches = new ArrayList();

			for (Map.Entry<String, Long> s : Main.index.entrySet()){
				if(s.getValue().equals(l))
					matches.add(s.getKey());
			}
			synchronized (sameSized) {
				sameSized.add(matches);
			}
		}
	}
}
