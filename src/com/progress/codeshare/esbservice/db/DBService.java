package com.progress.codeshare.esbservice.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;

import com.sonicsw.xq.*;
import com.sonicsw.xq.service.sj.MessageUtils;

public class DBService implements XQService {

	// This is the XQLog (the container's logging mechanism).
	private XQLog m_xqLog = null;

	// This is the the log prefix that helps identify this service during
	// logging
	private static String m_logPrefix = "";

	// These hold version information.
	private static int s_major = 1;

	private static int s_minor = 0;

	private static int s_buildNumber = 0;

	private static final String INPUT_RESULT = "Result";

	private static final String MODE_DISPATCH_API = "Dispatch API";

	private static final String NAMESPACE_URI = "http://www.progress.com/codeshare/esbservice/db";

	private static final String NAMESPACE_URI_DEFAULT_PROGRESS = "http://www.sonicsw.com/esb/service/dbservice";

	private static final String PARAM_NAME_INPUT = "input";

	private static final String PARAM_NAME_JDBC_DRIVER = "jdbcDriver";

	private static final String PARAM_NAME_MODE = "mode";

	private static final String PARAM_NAME_DEFAULT_PROGRESS = "defaultProgress";

	private static final String PARAM_NAME_PASSWORD = "password";

	private static final String PARAM_NAME_SQL_FILE = "sqlFile";

	private static final String PARAM_NAME_URL = "url";

	private static final String PARAM_NAME_USERNAME = "username";

	private String jdbcDriver;

	private String password;

	private String url;

	private String username;

	/**
	 * Constructor for a DBService
	 */
	public DBService() {
	}

	/**
	 * Initialize the XQService by processing its initialization parameters.
	 * 
	 * <p>
	 * This method implements a required XQService method.
	 * 
	 * @param initialContext
	 *            The Initial Service Context provides access to:<br>
	 *            <ul>
	 *            <li>The configuration parameters for this instance of the
	 *            DBServiceType.</li>
	 *            <li>The XQLog for this instance of the DBServiceType.</li>
	 *            </ul>
	 * @exception XQServiceException
	 *                Used in the event of some error.
	 */
	public void init(XQInitContext initialContext) throws XQServiceException {
		XQParameters params = initialContext.getParameters();
		m_xqLog = initialContext.getLog();
		setLogPrefix(params);

		m_xqLog.logInformation(m_logPrefix + " Initializing ...");

		writeStartupMessage(params);
		writeParameters(params);
		// perform initilization work.

		final String jdbcDriver = params.getParameter(PARAM_NAME_JDBC_DRIVER,
				XQConstants.PARAM_STRING);

		this.jdbcDriver = jdbcDriver;

		final String password = params.getParameter(PARAM_NAME_PASSWORD,
				XQConstants.PARAM_STRING);

		this.password = password;

		final String url = params.getParameter(PARAM_NAME_URL,
				XQConstants.PARAM_STRING);

		this.url = url;

		final String username = params.getParameter(PARAM_NAME_USERNAME,
				XQConstants.PARAM_STRING);

		this.username = username;

		m_xqLog.logInformation(m_logPrefix + " Initialized ...");

	}

	private void dbServiceServiceContext(XQServiceContext ctx)
			throws XQServiceException {

		try {

			final XQParameters params = ctx.getParameters();

			final String input = params.getParameter(PARAM_NAME_INPUT,
					XQConstants.PARAM_STRING);

			final String mode = params.getParameter(PARAM_NAME_MODE,
					XQConstants.PARAM_STRING);

			final boolean defaultProgress = params.getBooleanParameter(
					PARAM_NAME_DEFAULT_PROGRESS, XQConstants.PARAM_STRING);

			Connection conn = null;

			/* Ensure that the JDBC driver is loaded */
			Class.forName(jdbcDriver);

			Statement stmt = null;

			final String sqlFile = params.getParameter(PARAM_NAME_SQL_FILE,
					XQConstants.PARAM_STRING);

			ResultSet rs = null;

			final XQMessageFactory msgFactory = ctx.getMessageFactory();

			if (input.equals(INPUT_RESULT) && mode.equals(MODE_DISPATCH_API)) {
				final XQEnvelopeFactory envFactory = ctx.getEnvelopeFactory();

				final XQDispatch dispatcher = ctx.getDispatcher();

				while (ctx.hasNextIncoming()) {

					try {
						/* Connect to the DB */
						conn = DriverManager.getConnection(url, username,
								password);

						stmt = conn.createStatement();

						/* Execute the query */
						rs = stmt.executeQuery(sqlFile);

						final XQEnvelope origEnv = ctx.getNextIncoming();

						final XQMessage origMsg = origEnv.getMessage();

						final ResultSetMetaData rsMetaData = rs.getMetaData();

						while (rs.next()) {
							final XQEnvelope newEnv = envFactory
									.createDefaultEnvelope();

							final XQMessage newMsg = msgFactory.createMessage();

							/*
							 * Copy all headers from the original message to the
							 * new message
							 */
							MessageUtils.copyAllHeaders(origMsg, newMsg);

							/* Clear the Reply-To header to avoid failing back */
							newMsg.setReplyTo(null);

							final XQPart newPart = newMsg.createPart();

							newPart.setContentId("Result-0");

							final StringBuilder builder = new StringBuilder();

							builder
									.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

							if (!defaultProgress) {
								builder.append("<db:result xmlns:db=\""
										+ NAMESPACE_URI + "\">");
								builder.append("<db:resultSet>");
								builder.append("<db:row>");
							} else {
								builder
										.append("<db:result xmlns:db=\""
												+ NAMESPACE_URI_DEFAULT_PROGRESS
												+ "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
												+ " xsi:schemaLocation=\"http://www.sonicsw.com/esb/service/dbservice sonicfs:///System/Schemas/esb/service/DBService.xsd\">");

								builder
										.append("<db:resultSet version=\"1.1\">");
								builder.append("<db:row>");
							}

							for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
								final String columnName = rsMetaData
										.getColumnName(i);

								if (!defaultProgress) {
									builder.append("<db:"
											+ columnName.toUpperCase() + ">"
											+ rs.getString(i) + "</db:"
											+ columnName.toUpperCase() + ">");
								} else {
									builder.append("<"
											+ columnName.toUpperCase() + ">"
											+ rs.getString(i) + "</"
											+ columnName.toUpperCase() + ">");
								}
							}

							builder.append("</db:row>");

							builder.append("</db:resultSet>");

							builder.append("</db:result>");

							newPart.setContent(builder.toString(),
									XQConstants.CONTENT_TYPE_XML);

							newMsg.addPart(newPart);

							newEnv.setMessage(newMsg);

							dispatcher.dispatch(newEnv);
						}

					} finally {

						try {

							if (rs != null)
								rs.close();

						} finally {

							try {

								if (stmt != null)
									stmt.close();

							} finally {

								if (conn != null)
									conn.close();

							}

						}

					}

				}

			}

		} catch (final Exception e) {
			throw new XQServiceException(e);
		}
	}

	/**
	 * Handle the arrival of XQMessages in the INBOX.
	 * 
	 * <p>
	 * This method implement a required XQService method.
	 * 
	 * @param ctx
	 *            The service context.
	 * @exception XQServiceException
	 *                Thrown in the event of a processing error.
	 */
	public void service(XQServiceContext ctx) throws XQServiceException {

		if (ctx == null)
			throw new XQServiceException("Service Context cannot be null.");
		else {
			dbServiceServiceContext(ctx);
		}
	}

	/**
	 * Clean up and get ready to destroy the service.
	 * 
	 * <p>
	 * This method implement a required XQService method.
	 */
	public void destroy() {
		m_xqLog.logInformation(m_logPrefix + "Destroying...");

		m_xqLog.logInformation(m_logPrefix + "Destroyed...");
	}

	/**
	 * Called by the container on container start.
	 * 
	 * <p>
	 * This method implement a required XQServiceEx method.
	 */
	public void start() {
		m_xqLog.logInformation(m_logPrefix + "Starting...");

		m_xqLog.logInformation(m_logPrefix + "Started...");
	}

	/**
	 * Called by the container on container stop.
	 * 
	 * <p>
	 * This method implement a required XQServiceEx method.
	 */
	public void stop() {
		m_xqLog.logInformation(m_logPrefix + "Stopping...");

		m_xqLog.logInformation(m_logPrefix + "Stopped...");
	}

	/**
	 * Clean up and get ready to destroy the service.
	 * 
	 */
	protected void setLogPrefix(XQParameters params) {
		String serviceName = params.getParameter(
				XQConstants.PARAM_SERVICE_NAME, XQConstants.PARAM_STRING);
		m_logPrefix = "[ " + serviceName + " ]";
	}

	/**
	 * Provide access to the service implemented version.
	 * 
	 */
	protected String getVersion() {
		return s_major + "." + s_minor + ". build " + s_buildNumber;
	}

	/**
	 * Writes a standard service startup message to the log.
	 */
	protected void writeStartupMessage(XQParameters params) {
		
		final StringBuffer buffer = new StringBuffer();
		
		String serviceTypeName = params.getParameter(
				XQConstants.SERVICE_PARAM_SERVICE_TYPE,
				XQConstants.PARAM_STRING);
		
		buffer.append("\n\n");
		buffer.append("\t\t " + serviceTypeName + "\n ");
		
		buffer.append("\t\t Version ");
		buffer.append(" " + getVersion());
		buffer.append("\n");
				
		buffer.append("\t\t Copyright (c) 2008, Progress Sonic Software Corporation.");
		buffer.append("\n");
		
		buffer.append("\t\t All rights reserved. ");
		buffer.append("\n");
		
		m_xqLog.logInformation(buffer.toString());
	}

	/**
	 * Writes parameters to log.
	 */
	protected void writeParameters(XQParameters params) {

		final Map map = params.getAllInfo();
		final Iterator iter = map.values().iterator();

		while (iter.hasNext()) {
			final XQParameterInfo info = (XQParameterInfo) iter.next();

			if (info.getType() == XQConstants.PARAM_XML) {
				m_xqLog.logDebug(m_logPrefix + "Parameter Name =  "
						+ info.getName());
			} else if (info.getType() == XQConstants.PARAM_STRING) {
				m_xqLog.logDebug(m_logPrefix + "Parameter Name = "
						+ info.getName());
			}

			if (info.getRef() != null) {
				m_xqLog.logDebug(m_logPrefix + "Parameter Reference = "
						+ info.getRef());

				// If this is too verbose
				// /then a simple change from logInformation to logDebug
				// will ensure file content is not displayed
				// unless the logging level is set to debug for the ESB
				// Container.
				m_xqLog.logDebug(m_logPrefix
						+ "----Parameter Value Start--------");
				m_xqLog.logDebug("\n" + info.getValue() + "\n");
				m_xqLog.logDebug(m_logPrefix
						+ "----Parameter Value End--------");
			} else {
				m_xqLog.logDebug(m_logPrefix + "Parameter Value = "
						+ info.getValue());
			}
		}
	}

}