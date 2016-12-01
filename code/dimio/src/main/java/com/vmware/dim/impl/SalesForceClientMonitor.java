package com.vmware.dim.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SalesForceClientMonitor implements Runnable {

	private static final Logger logger = LogManager.getLogger(SalesForceClientMonitor.class);

	private SalesforceInputStream salesforceInputStream;

	public SalesForceClientMonitor(SalesforceInputStream salesforceInputStream) {
		this.salesforceInputStream = salesforceInputStream;
	}

	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(10 * 1000);

				logger.debug(salesforceInputStream.getChannel() + "[BAYEUX CLIENT ] bayeuxClient.isConnected() -> " + salesforceInputStream.getBayeuxClient().isConnected());
				logger.debug(salesforceInputStream.getChannel() +"[BAYEUX CLIENT ] bayeuxClient.isDisconnected() -> " + salesforceInputStream.getBayeuxClient().isDisconnected());
				logger.debug(salesforceInputStream.getChannel() +"[BAYEUX CLIENT ] bayeuxClient.isHandshook() -> " + salesforceInputStream.getBayeuxClient().isHandshook());

				if (!salesforceInputStream.getBayeuxClient().isConnected()) {
					logger.debug("Re-connectingclient");
					salesforceInputStream.disconnectClient();
					logger.debug("Disconnected client");
					salesforceInputStream.connectClient();
					logger.debug("Re-Connected client");
				}
			} catch (Exception e) {
				logger.error("Error monitoring BayeuxClient", e);
			}
		}

	}

}
