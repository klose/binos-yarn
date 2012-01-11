package binos.yarn;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

class  BinosYarnClient {
  private static final Log LOG = LogFactory.getLog( BinosYarnClient.class);
  private static final String appName = "Binos-Yarn";
  private static final int yarn_fail_count = 4;
  private String binosHome;
  //private String sparkHome;
  private int numSlaves;
  private int slaveMem;
  private int slaveCpus;
  private int masterMem;
  private int masterPort;
  private int slavePort;
  private String logDirectory;
  private String programArgs;
  
  private Configuration conf;
  private YarnRPC rpc;
  private ClientRMProtocol applicationsManager;

  public  BinosYarnClient(String[] args) {
    try {
      Options opts = new Options();
      opts.addOption("binos_home", true, "directory where Binos is installed");
      opts.addOption("slaves", true, "number of slaves");
      opts.addOption("slave_mem", true, "memory per slave, in MB (default: 32)");
      opts.addOption("master_mem", true, "memory for master, in MB (default: 256)");
      opts.addOption("master_port", true, "port for master service (default: 6060)");
      opts.addOption("slave_cpus", true, "CPUs to use per slave (default: 1)");
      opts.addOption("slave_port", true, "port for slave service (default: 6061)");
      opts.addOption("log_dir", true, "log directory for Application Master");
      opts.addOption("args", true, "arguments to pass to main class");
      opts.addOption("help", false, "print this help message");
      
      CommandLine line = new GnuParser().parse(opts, args);
      
      if (args.length == 0 || line.hasOption("help")) {
        new HelpFormatter().printHelp("client", opts);
        System.exit(0);
      }
      if ((binosHome = line.getOptionValue("binos_home", System.getenv("BINOS_HOME"))) == null) {
        System.err.println("BINOS_HOME needs to be specified, either as a command line " +
            "option or through the environment");
        System.exit(1);
      }
      if ((numSlaves = Integer.parseInt(line.getOptionValue("slaves", "-1"))) == -1) {
        System.err.println("Number of slaves needs to be specified, using -slaves");
        System.exit(1);
      }
      if ((logDirectory = line.getOptionValue("log_dir", System.getenv("BINOS_YARN_LOGDIR"))) == null) {
    	  System.err.println("BINOS_YARN_LOGDIR needs to be specified, either as a command line " +
            "option or through the environment");
    	  System.exit(1);
      }
      slaveMem = Integer.parseInt(line.getOptionValue("slave_mem", "32"));
      slaveCpus = Integer.parseInt(line.getOptionValue("slave_cpus", "1"));
      masterMem = Integer.parseInt(line.getOptionValue("master_mem", "256"));
      masterPort = Integer.parseInt(line.getOptionValue("master_port", "6060"));
      slavePort = Integer.parseInt(line.getOptionValue("slave_port", "6061"));
      programArgs = line.getOptionValue("args", "");
      LOG.info("programArgs: " + programArgs);
    } catch (ParseException e) {
      System.err.println("Failed to parse command line options: " + e.getMessage());
      System.exit(1);
    }
  }

  public static void main(String[] args) throws Exception {
    new  BinosYarnClient(args).run();
  }

  public void run() throws Exception {
    conf = new YarnConfiguration();
    rpc = YarnRPC.create(conf);
    
    InetSocketAddress rmAddress = 
    		NetUtils.createSocketAddr(this.conf.get(
    				YarnConfiguration.RM_ADDRESS,
    				YarnConfiguration.DEFAULT_RM_ADDRESS),
    				YarnConfiguration.DEFAULT_RM_PORT,
    				YarnConfiguration.RM_ADDRESS);
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    applicationsManager = 
      (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, conf);

    ApplicationId appId = newApplicationId();
    LOG.info("Got application ID " + appId);


    ApplicationSubmissionContext ctx = createApplicationSubmissionContext(conf, appId);
    
    // Launch the application master
    SubmitApplicationRequest request = Records.newRecord(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(ctx);
    applicationsManager.submitApplication(request);
    LOG.info("Submitted application to ResourceManager");
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(
    Configuration conf, ApplicationId appId) throws Exception {
    
	ApplicationSubmissionContext asc = Records.newRecord(ApplicationSubmissionContext.class);
    asc.setApplicationId(appId);
    asc.setApplicationName(appName);
    
    ContainerLaunchContext amContainer =
    		Records.newRecord(ContainerLaunchContext.class);
    
    // Define the local resources required
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    
    //set the resource for this Container.
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(masterMem);
    amContainer.setResource(capability);
    LOG.info("AppMaster capability = " + capability);

    // Add command line
    String home = System.getenv("BINOS_YARN_HOME");
    if (null == home) {
    	home = new File(".").getAbsolutePath();
    }
    String command =  home +  "/bin/application-master " +
    		"-yarn_timestamp " + appId.getClusterTimestamp() + " " +
            "-yarn_id " + appId.getId() + " " +
            "-yarn_fail_count " + yarn_fail_count + " " +
            "-binos_home " + binosHome + " " +
            "-slaves " + numSlaves + " " +
            "-slave_mem " + slaveMem + " " +
            "-slave_cpus " + slaveCpus + " " +
            "-master_mem " + masterMem + " " +
            "-master_port " + masterPort + " " +
            "-slave_port " + slavePort + " " +
            (programArgs.equals("") ? "" : "-args " + BinosYarnUtils.quote(programArgs) + " ") +
             "-log_dir " + logDirectory + " " +
             "1>" + logDirectory + "/stdout " +
             "2>" + logDirectory + "/stderr"; 
    System.out.println(command);
    List<String> commands = new ArrayList<String>();
    commands.add(command);
    amContainer.setCommands(commands);
    
    // set AMContainer for ApplicationSubmissionContext
    asc.setAMContainerSpec(amContainer);
    asc.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    return asc;
  }

  private ApplicationId newApplicationId() throws YarnRemoteException {
    GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
    return applicationsManager.getNewApplication(request).getApplicationId();
  }
}
