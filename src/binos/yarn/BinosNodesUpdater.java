package binos.yarn;

import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.googlecode.protobuf.socketrpc.SocketRpcServer;

/**
 * BinosNodesUpdater: responsible for fetching the binos status 
 * @author jiangbing
 *
 */
public class BinosNodesUpdater {
	private Log LOG = LogFactory.getLog(BinosNodesUpdater.class.getName());
	private final int rpcServerPort;
	private final static BinosResourceReqService reqService = new BinosResourceReqService(); 
	BinosNodesUpdater(int port) {
		this.rpcServerPort = port;
	}
	public void initService() {
		SocketRpcServer masterServer = new SocketRpcServer(this.rpcServerPort,
			    Executors.newFixedThreadPool(4));
		masterServer.registerService(reqService);
		masterServer.startServer();
	}
	public static int getRequestAmount() {
		return reqService.getRequestAmount();
	}
	/**
	 * feedback to resource request. Meaning trigger adjust slave.
	 */
	public static void feedback() {
		reqService.reCountRequestAmount();
	}
}


