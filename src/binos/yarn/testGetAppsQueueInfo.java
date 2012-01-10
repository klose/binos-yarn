package binos.yarn;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;

public class testGetAppsQueueInfo {
	private static final Log LOG = LogFactory.getLog(testGetAppsQueueInfo.class);
	private final ClientRMProtocol crmDelegation;
	private final Configuration conf;
	private final YarnRPC rpc;
	testGetAppsQueueInfo() {
		conf = new YarnConfiguration();
		rpc = YarnRPC.create(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(this.conf.get(
				YarnConfiguration.RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_ADDRESS),
				YarnConfiguration.DEFAULT_RM_PORT,
				YarnConfiguration.RM_ADDRESS);
		
		     
		    
		
		//amrmDelegation = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf);
		crmDelegation = (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, conf);
	}
	
    public void getQueueStatus() {
		GetQueueInfoRequest request = Records.newRecord(GetQueueInfoRequest.class);
		GetQueueInfoResponse queueRep;
		try {
			queueRep = crmDelegation.getQueueInfo(request);
		
			QueueInfo qi = queueRep.getQueueInfo();
			List<ApplicationReport> appsList = qi.getApplications();
			LOG.info("Application number:" + appsList.size());
			for (ApplicationReport ar : appsList) {
				LOG.info(
						ar.getApplicationId() + " state:"
						+ ar.getYarnApplicationState().toString() +" "
						+ "Start Time:" + ar.getStartTime() + " "
						);
			}
		} catch (YarnRemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void main(String [] args) {
		
		Thread statusMonitor = new Thread(new Runnable(){
			testGetAppsQueueInfo testStatus = new testGetAppsQueueInfo();
			@Override
			public void run() {
				// TODO Auto-generated method stub
				while(true) {
					LOG.info("before");
					testStatus.getQueueStatus();
					LOG.info("after");
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
