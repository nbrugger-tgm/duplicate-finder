package com.company;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Queue;
import java.util.*;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

public class Main {
	//settings
	static final int buffer = 1024 * 8;
	static final int cores = Runtime.getRuntime().availableProcessors();
	static final int minStackSize = (int) ((Runtime.getRuntime().maxMemory() / 1000) / new String(new char[100]).getBytes().length);
	static final int stackStop = (minStackSize * 2);

	//complex runtime
	static ThreadGroup indexers = new ThreadGroup("Indexers");
	static ThreadGroup groupers = new ThreadGroup("SizeGroupers");
	static ThreadGroup compareGroup = new ThreadGroup("Comparators");
	static Queue<File> foldersToIndex = new LinkedList<>();

	//data
	static Map<String, Long> index;
	static Map<String, Exception> exceptions = new HashMap<>();
	static List<List<String>> sameSized = new ArrayList<>();
	static List<List<String>> sameContent = new ArrayList<>();
	static DB database;
	//meta data
	static long indexSize;
	static boolean onlyConsumeStack = false;
	static int checkedFiles = 0;
	static long sameSizeFiles = 0;
	static Settings settings = new Settings();

	static class Settings {
		public boolean permanentIndex = false;
		public boolean useFile = Runtime.getRuntime().freeMemory()>1024*1024*1024*8;
		public File permaLocation = new File(System.getProperty("user.home"),"duplicate-finder-index.map");
		public boolean rebuild = false;
		public String[] exts = {"doc", "docx", "pdf"/*,"png","jpg","jpeg"*/};
	}

	public static void main(String[] args) {
		Scanner sysin = new Scanner(System.in);
		System.out.println("This is the setup of duplicate finder.\n");
		System.out.print("Do you like to use custom settings? [Y/N] (Default: No) > ");
		boolean alter = sysin.nextLine().toLowerCase().equals("y");

		File[] roots;
		if(alter){
			ArrayList<String> paths = new ArrayList<>(5);
			String path;
			System.out.println("\nDevices/Folders to scan :");
			roots = readUserList(sysin, File::new, i->new File[i]);

			ArrayList<String> extensions = new ArrayList<>(5);
			String ext;
			System.out.println("\nFile Types (without the dot -> eg: docx) :");
			settings.exts = readUserList(sysin, s->s,i->new String[i]);

			double ramGB = (Runtime.getRuntime().freeMemory()/(double)1024*1024*1024);
			ramGB -= ramGB%0.1;
			System.out.println(
				"\n" +
				"The app creates an index with all your files information in it.\n" +
				"When you have a lot files this index might get very big (Up to many GBs)\n" +
				"Java has access to "+ramGB+" GB RAM. If you have many files you should store the index on your harddrive.\n" +
				"Use a file if you got a \"StackOverflow\" or \"OutOfMemory\" Error/Exception you should answer with yes."
			);
			System.out.print("Use a file for the index? [Y/N] (Default: "+(settings.useFile?"yes":"no")+") > ");
			String answer = sysin.nextLine().toLowerCase();
			settings.useFile = answer.matches("[yYnN]") ? answer.equals("y") : settings.useFile;

			System.out.print("\nKeep the index after the script is done ? [y/n] (Default: "+(settings.permanentIndex?"yes":"no")+") > ");
			answer = sysin.nextLine().toLowerCase();
			settings.permanentIndex = answer.matches("[yYnN]") ? answer.equals("y") : settings.permanentIndex;

			if(settings.permanentIndex){
				System.out.print("\nIndex File location? (Default: "+(settings.permaLocation)+") > ");
				answer = sysin.nextLine();
				if(answer.length()>0)
					settings.permaLocation = new File(answer);
			}
			if(settings.permaLocation.exists()){
				System.out.println("The index file already exits, you can use it and skip the index building.");
				System.out.print("\nUse the old index? [y/n] (Default: "+(settings.rebuild?"no":"yes")+" > ");
				answer = sysin.nextLine().toLowerCase();
				settings.rebuild = answer.matches("[yYnN]") ? !answer.equals("y") : settings.rebuild;
			}
		}else{
			if(args.length == 0) {
				roots = File.listRoots();
			}else{
				roots = Arrays.stream(args).map(s -> new File(s)).toArray(s -> new File[s]);
			}
		}

		if(settings.useFile){
			if(settings.permaLocation.exists() && settings.rebuild)
				settings.permaLocation.delete();
			database = DBMaker.fileDB(settings.permaLocation).allocateStartSize(Runtime.getRuntime().freeMemory()*2).closeOnJvmShutdown().executorEnable().make();
			index = database.hashMap("index", Serializer.STRING,Serializer.LONG).createOrOpen();
		}else{
			index = new HashMap<>();
		}
		System.out.println("\nStart ...\n");
		if(index.size() == 0)
       		createIndex(roots);
        findSizeCorelation();
        compareContent();
        printResults();
	}

	@NotNull
	private static<T> T[] readUserList(Scanner sysin, Function<String, T> converter, IntFunction<T[]> arrayInit) {
		String ext;
		T[] roots;
		ArrayList<String> options = new ArrayList<>();
		do {
			System.out.print("\t> ");
			ext = sysin.nextLine();
			if(ext.length() > 0)
				options.add(ext);
		}while (ext.length() > 0);
		roots = options.stream().map(converter).toArray(arrayInit);
		return roots;
	}

	private static void printResults() {
		if (GraphicsEnvironment.isHeadless()) {
			for (List<String> files : sameContent) {
				for (String file : files) {
					System.out.println(file);
				}
				System.out.println("---------------------------");
			}
			System.out.println("Fehler : " + exceptions.size());
		} else {
			JFileChooser jfc = new JFileChooser();
			jfc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
			jfc.showDialog(null, "Speichern");
			File f = jfc.getSelectedFile();
			if (f == null)
				JOptionPane.showMessageDialog(null, "Daten wurde nicht gespeichert");
			File errors = new File(f, "errors.txt");
			File output = new File(f, "scan-result.txt");
			try {
				errors.createNewFile();
				PrintWriter errorWriter = new PrintWriter(errors);
				for (Map.Entry<String, Exception> stringExceptionEntry : exceptions.entrySet()) {
					errorWriter.println(stringExceptionEntry.getKey() + " : ");
					stringExceptionEntry.getValue().printStackTrace(errorWriter);
				}
				errorWriter.flush();
				errorWriter.close();
			} catch (IOException e) {
				JOptionPane.showMessageDialog(null, e);
			}
			try {
				output.createNewFile();
				PrintWriter outWriter = new PrintWriter(output);
				for (List<String> files : sameContent) {
					for (String file : files) {
						outWriter.println(file);
					}
					outWriter.println("---------------------------");
				}
				outWriter.flush();
				outWriter.close();
			} catch (IOException e) {
				JOptionPane.showMessageDialog(null, e);
			}

		}
	}

	private static void compareContent() {
		System.out.println("\nFind duplicates");
		long start = System.currentTimeMillis();
		ContentCompareThread[] workers = new ContentCompareThread[cores];
		for (int i = 0; i < workers.length; i++) {
			workers[i] = new ContentCompareThread(compareGroup);
			workers[i].start();
		}
		while (compareGroup.activeCount() > 0) {
			System.gc();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			long files = 0;
			for (ContentCompareThread t : workers) {
				synchronized (t.matches) {
					files += t.matches.stream().flatMap(yeet -> yeet.stream()).count();
				}
			}
			System.out.print("\rDuplicate files : " + files+"                                            ");
			System.out.print("\rChecked Files : " + checkedFiles + "/" + (sameSizeFiles + "(" + (checkedFiles / ((double) sameSizeFiles)) * 100) + "%)                              ");
		}
		for (int i = 0; i < workers.length; i++) {
			sameContent.addAll(workers[i].matches);
		}
		workers = null;
		long end = System.currentTimeMillis();
		System.out.println("\n+--------- Done");
		System.out.println("| Time: " + ((end - start) / 1000) + "s");
		System.gc();
	}

	private static void findSizeCorelation() {
		System.out.println("\nGroup files by their size");
		long start = System.currentTimeMillis();
		int chunckSize = (int) Math.max(1, (indexSize / cores));
		List<Long> sizes = new ArrayList<>();
		sizes.addAll(index.values());
		Set<Long> uniqueSizes = sizes.stream().collect(Collectors.toSet());
		sizes = null;
		List<Set<Long>> splitedSizes = new ArrayList<>();
		Set<Long> subsset = null;
		int pos = 0;
		for (Long size : uniqueSizes) {
			if (pos % chunckSize == 0) {
				subsset = new HashSet<>();
				splitedSizes.add(subsset);
			}
			subsset.add(size);
			pos++;
		}
		subsset = null;
		uniqueSizes = null;
		System.gc();

		System.out.println("    Start processing");
		SizeMatchingThread[] workers = new SizeMatchingThread[splitedSizes.size()];
		for (int i = 0; i < workers.length; i++) {
			Set<Long> subset = splitedSizes.get(i);
			workers[i] = new SizeMatchingThread(groupers, subset, i);
			workers[i].start();
		}
		while (groupers.activeCount() > 0) {
			long files = 0;
			for (SizeMatchingThread t : workers) {
				synchronized (t.sameSized) {
					files += t.sameSized.stream().flatMap(yeet -> yeet.stream()).count();
				}
			}
			System.out.printf("\r    Sizematch : %s files                                           ", files);
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
		}
		for (int i = 0; i < workers.length; i++) {
			sameSized.addAll(workers[i].sameSized);
		}
		sameSized = sameSized.stream().filter(e -> e.size() > 1).collect(Collectors.toList());
		long end = System.currentTimeMillis();
		System.out.println("\n+--------- Done");
		System.out.println("| Time: " + ((end - start) / 1000) + " s");
		long files = sameSized.stream().flatMap(yeet -> yeet.stream()).count();
		System.out.printf("| Same-sized Files : %s files\n", files);
		sameSizeFiles = files;
		indexSize = files;
		indexers = null;
		index = null;
		workers = null;
		System.gc();
	}

	private static void createIndex(File... roots) {
		System.out.println("Create index (reading files and sizes)");
		long start = System.currentTimeMillis();
		for (File f : roots)
			addIndexTask(f);


		IndexingThread[] workers = new IndexingThread[cores];
		for (int i = 0; i < cores; i++) {
			workers[i] = new IndexingThread(indexers, "IndexBuilder " + i);
			workers[i].start();
			try {
				workers[i].join(500);
			} catch (InterruptedException e) {
			}
		}


		while (indexers.activeCount() > 0) {
			long size = 0;
			for (IndexingThread worker : workers) {
				size += worker.localIndex.size();
			}
			System.out.printf("%s files                                               \r", size);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}


		for (IndexingThread t : workers) {
			index.putAll(t.localIndex);
		}
		long end = System.currentTimeMillis();
		System.out.println("\n+--------- Done");
		System.out.println("| Time: " + ((end - start) / 1000) + " s");
		System.out.printf("| Index-Size: %s files\n", index.size());
		indexSize = index.size();
		indexers = null;
		workers = null;
		System.gc();
	}

	public static File getNextIndexTask() {
		synchronized (foldersToIndex) {
			return foldersToIndex.poll();
		}
	}

	public static boolean addIndexTask(File sub) {
		int size = foldersToIndex.size();
		if (size >= stackStop)
			onlyConsumeStack = true;
		boolean bigger = size > minStackSize;
		if (!bigger)
			onlyConsumeStack = false;
		if (onlyConsumeStack && bigger)
			return false;
		synchronized (foldersToIndex) {
			foldersToIndex.add(sub);
		}
		return true;
	}

	public static List<String> getNextCompareFiles() {
		synchronized (sameSized) {
			try {
				int last = sameSized.size() - 1;
				List<String> list = sameSized.get(last);
				sameSized.remove(last);
				return list;
			} catch (IndexOutOfBoundsException e) {
				return null;
			}
		}
	}
}
