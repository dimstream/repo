package com.vmware.common.action.impl;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.action.PlayAction;
import com.vmware.common.dao.Action;
import com.vmware.common.dto.ActionDTO;
import com.vmware.common.dto.ConfigurationDTO;

/**
 * Action class implementing {@link Action} interface to
 * send email notifications to the list of configured email ids.
 * 
 * @author vedanthr
 */
public class EmailNotificationAction extends PlayAction {

	private static final Logger logger = LogManager.getLogger(EmailNotificationAction.class);

	/**
	 * Send email to configured list by through the configured SMTP client.
	 * 
	 * @param actionDTO Email Notification Configuration Details
	 * @param data Data from stream
	 */
	public boolean doAction(ActionDTO actionDTO, String data) {
		logger.traceEntry(data);
		String subject = "";
		String mailTo = "";
		for (ConfigurationDTO configurationDTO : actionDTO.configuration) {
			logger.debug(configurationDTO.key + " -- " + configurationDTO.value);
			if ("Subject".equals(configurationDTO.key))
				subject = configurationDTO.value;
			if ("Mail To".equals(configurationDTO.key))
				mailTo = configurationDTO.value;
		}

		logger.debug("EmailNotificationAction");
		try {
			Email email = new SimpleEmail();
			email.setHostName("smtp.googlemail.com");
			email.setSmtpPort(465);
			email.setAuthenticator(new DefaultAuthenticator("", ""));
			email.setSSLOnConnect(true);
			email.setFrom("");
			email.setSubject(subject);
			email.setMsg(data);
			email.addTo(mailTo);
			email.send();
		} catch (EmailException emailException) {
			logger.error(emailException);
		}
		return true;
	}
}
