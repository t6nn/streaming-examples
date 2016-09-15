package eu.t6nn.tutorials;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Farmer who farms a specific type of fruit.
 */
public class Farmer implements Runnable {

	private final static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
	private final static Random rnd = new Random();

	private final File logFile;
	private final String fruitType;

	private Farmer( String fruitType, File logFile ) throws FileNotFoundException {
		this.fruitType = fruitType;
		this.logFile = logFile;
	}

	public void run() {
		System.out.println( "Picked 1 " + fruitType );
		try {
			FileUtils.writeStringToFile(logFile, fruitType + " " + System.currentTimeMillis() + "\n", "UTF-8", true);
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
		executor.schedule( this, 100 + rnd.nextInt( 5000 ), TimeUnit.MILLISECONDS );
	}

	public static void main( String[] args ) throws FileNotFoundException {
		if ( args.length < 1 || args.length > 2 ) {
			throw new IllegalArgumentException( "Usage: Farmer [fruit] log_file" );
		}
		if(args.length == 2) {
			executor.submit(new Farmer(args[0], new File(args[1])));
		} else {
			executor.submit(new Farmer(rnd.nextBoolean() ? "orange" : "apple", new File(args[0])));
		}
	}
}
