package com.company;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexingThread extends Thread{

	Map<String,Long> localIndex = new HashMap<>();
	static long currentIndexPage = 0;
	public IndexingThread(ThreadGroup indexers, String s) {
		super(indexers,s);
	}

	@Override
	public void run() {
		File folder;
		while((folder = Main.getNextIndexTask()) != null){
			processFolder(folder);
		}
	}

	private void processFolder(File folder) {
		File[] subs;
		try {
			subs = folder.listFiles(file -> {
				if(file.isDirectory())
					return true;
				boolean extR = false;
				for (String ext : Main.settings.exts) {
					if(file.getAbsolutePath().toLowerCase().endsWith(ext)){
						extR = true;
						break;
					}
				}

				return extR;
			});
			if (subs == null)
				throw new IOException("Cant list content of "+folder);
		}catch(SecurityException | IOException e){
			Main.exceptions.put(folder.getAbsolutePath(), e);
			return;
		}
		for (File sub : subs){
			if(sub.isFile()){
				localIndex.put(sub.getAbsolutePath(), sub.length());
				currentIndexPage++;
			}else{
				if(!Main.addIndexTask(sub)){
					processFolder(sub);
				}
			}
		}
	}
}
