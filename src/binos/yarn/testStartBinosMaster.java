package binos.yarn;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.SynchronousQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


//import com.google.common.collect.Lists;
public class testStartBinosMaster {
  // Start binos-master and read its URL into binosUrl
  
  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
	private static Process master;
	private static final String logDirectory = "/home/jiangbing/Master-Slave/logs/"; 
public static void startBinosMaster() throws IOException {
    String masterUrl = null;
//	String logDirectory = "/home/jiangbing/Master-Slave/logs";	
	String[] command = new String[] { "/home/jiangbing/Master-Slave/bin/binos-master.sh",
        "--port=60110", "--log_dir=" + logDirectory};
	System.out.println(Arrays.toString(command));
    master = Runtime.getRuntime().exec(command);
    final SynchronousQueue<String> urlQueue = new SynchronousQueue<String>();

    // Start a thread to redirect the process's stdout to a file
    new Thread("stdout redirector for binos-master") {
      public void run() {
        BufferedReader in = new BufferedReader(new InputStreamReader(master.getInputStream()));
        PrintWriter out = null;
        try {
          out = new PrintWriter(new FileWriter(logDirectory + "/binos-master.stdout"));
          String line = null;
          while ((line = in.readLine()) != null) {
            out.println(line);
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          if (out != null)
            out.close();
        }
      }
    }.start();

    // Start a thread to redirect the process's stderr to a file and also read the URL
    new Thread("stderr redirector for binos-master") {
      public void run() {
		BufferedReader in = null;
    	try {
      		Thread.sleep(500); // Give binos-master a bit more time to start up
    	} catch (InterruptedException e) {}
		
		try {
		 in = new BufferedReader(new FileReader(logDirectory+"/binos-master.log"));
       // BufferedReader in = new BufferedReader(new InputStreamReader(master.getErrorStream()));
       // PrintWriter out = null;
        //try {
          //out = new PrintWriter(new FileWriter(logDirectory + "/binos-master.log"));
          boolean foundUrl = false;
          Pattern pattern = Pattern.compile(".*Master started at (.*)");
          String line = null;
          while ((line = in.readLine()) != null) {
            //out.println(line);
            if (!foundUrl) {
              Matcher m = pattern.matcher(line);
              if (m.matches()) {
                String url = m.group(1);
                urlQueue.put(url);
                foundUrl = true;
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
        //	if (in != null)
          //  in.close();
        }
      }
    }.start();
    
    // Wait until we've read the URL
    while (masterUrl == null) {
      try {
        masterUrl = urlQueue.take();
      } catch (InterruptedException e) {}

    LOG.info("Binos master started with URL " + masterUrl);
    try {
      Thread.sleep(500); // Give binos-master a bit more time to start up
    } catch (InterruptedException e) {}
  }
}
public static void main(String[] args) throws Exception {
	startBinosMaster();
}
}
