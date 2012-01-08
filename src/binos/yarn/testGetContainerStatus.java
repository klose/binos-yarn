package binos.yarn;

import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

public class testGetContainerStatus {
	private static final Log LOG = LogFactory.getLog(testGetContainerStatus.class);
	private final AMRMProtocol amrmDelegation;
	private final ClientRMProtocol crmDelegation;
	private final Configuration conf;
	private final YarnRPC rpc;
	testGetContainerStatus() {
		conf = new YarnConfiguration();
		rpc = YarnRPC.create(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(this.conf.get(
				YarnConfiguration.RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_ADDRESS),
				YarnConfiguration.DEFAULT_RM_PORT,
				YarnConfiguration.RM_ADDRESS);
		
		     
		    
		
		amrmDelegation = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf);
		crmDelegation = (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, conf);
	}
	/*
	 * Get all NM status.
	 * */
	public void getClusterStatus() {
		GetClusterNodesRequest nodesRequest = Records.newRecord(GetClusterNodesRequest.class);
		try {
			LOG.info("crmDelegation.toString:" + crmDelegation.toString());
			
			List<NodeReport> nodesList  = crmDelegation.getClusterNodes(nodesRequest).getNodeReports();
			//StringBuilder info = new StringBuilder();
			StringBuilder info = new StringBuilder();
			info.append('\n');
			for (NodeReport nr: nodesList) {				
				info.append("NodeID:");
				info.append(nr.getNodeId());
				info.append('\t');
				info.append("HttpAddress:");
				info.append(nr.getHttpAddress());
				info.append('\t');
				info.append("NumContainers:");
				info.append(nr.getNumContainers());
				info.append('\t');
				info.append("RackName:");
				info.append(nr.getRackName());
				info.append('\t');
				info.append("memory:");
				info.append(nr.getCapability().getMemory());
				info.append('\t');
				info.append("HealthReport:");
				info.append(nr.getNodeHealthStatus().getHealthReport());
				info.append('\n');
			}
			LOG.info(info.toString());
		} catch (YarnRemoteException e) {
			// TODO Auto-generated catch block
			LOG.error("Client cannot connect to ResourceManager. " + e.getMessage());
			e.printStackTrace();	
		}
	}
	public void getClusterMetrics()  {
		GetClusterMetricsRequest request = Records.newRecord(GetClusterMetricsRequest.class);
		
		
		//GetClusterMetricsResponse response = crmDelegation.getClusterMetrics(request);
		
		
	}
	public static void main(String [] args) {
		
		Thread statusMonitor = new Thread(new Runnable(){
			testGetContainerStatus testStatus = new testGetContainerStatus();
			@Override
			public void run() {
				// TODO Auto-generated method stub
				while(true) {
					testStatus.getClusterStatus();
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
		});
		statusMonitor.start();
	}
	
	
}
