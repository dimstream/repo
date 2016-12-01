package com.vmware.point;

import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ContextMappingDefinitionDTO;
import com.vmware.common.dto.EventStreamDTO;
import com.vmware.common.dto.ProcessContextDTO;
import com.vmware.common.dto.TrapDTO;
import com.vmware.manage.Manager;

/**
 * Rest End Points for the all the services provided by Manager API
 * 
 * @author ghimanshu
 *
 */
@Path("/")
public class RestEndPoints {

	private static final Logger logger = LogManager.getLogger(RestEndPoints.class);
	Gson gson = new Gson();

	/**
	 * Method to Upsert Event Stream into DB
	 * 
	 * @param request Json
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/eventstream")
	public String upsertStream(String request) {
		try {
			logger.traceEntry(request);
			EventStreamDTO dto = Manager.upsertStream(request);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in upserting eventstream in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Retrieve Event Stream from DB
	 * 
	 * @param eventStreamName EventStreamName
	 * @return Successful Json or Unsuccessful Message
	 */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("eventstream/{eventStreamName}")
	public String findStream(@PathParam("eventStreamName") String eventStreamName) {
		// Show something
		try {
			logger.traceEntry(eventStreamName);
			if (null == eventStreamName || eventStreamName.isEmpty()) {
				logger.error("Invalid key entered");
				throw new Exception();
			}
			EventStreamDTO dto = Manager.findStream(eventStreamName);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in reading eventstream in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Delete Event Stream from DB
	 * 
	 * @param eventStreamName EventStreamName
	 * @return 1(true) or Error Message
	 */
	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/eventstream/{eventStreamName}")
	public String removeStream(@PathParam("eventStreamName") String eventStreamName) {
		// Annihilate something
		try {
			logger.traceEntry(eventStreamName);
			Integer deleteStatus = Manager.removeStream(eventStreamName);
			return logger.traceExit(deleteStatus.toString());
		} catch (Exception e) {
			logger.error("Error in deleting eventstream in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Upsert Context into DB
	 * 
	 * @param request Json
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/context")
	public String upsertContext(String request) {
		// Create something
		try {
			logger.traceEntry(request);
			ProcessContextDTO dto = Manager.upsertContext(request);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in upserting context in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Retrieve Context from DB
	 * 
	 * @param processContextName ContextName
	 * @return Successful Json or Unsuccessful Message
	 */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/context/{processContextName}")
	public String findContext(@PathParam("processContextName") String processContextName) {
		// Show something
		try {
			logger.traceEntry(processContextName);
			if (null == processContextName || processContextName.isEmpty()) {
				logger.error("Invalid key entered");
				throw new Exception();
			}
			ProcessContextDTO dto = Manager.findContext(processContextName);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in reading context in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Delete Context from DB
	 * 
	 * @param processContextName ContextName
	 * @return 1(true) or Error Message
	 */
	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/context/{processContextName}")
	public String removeContext(@PathParam("processContextName") String processContextName) {
		// Annihilate something
		try {
			logger.traceEntry(processContextName);
			Integer deleteStatus = Manager.removeContext(processContextName);
			return logger.traceExit(deleteStatus.toString());
		} catch (Exception e) {
			logger.error("Error in deleting context in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Upsert Mapping into DB
	 * 
	 * @param request Json
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/mapping")
	public String upsertStreamMapping(String request) {
		// Create something
		try {
			logger.traceEntry(request);
			ContextMappingDefinitionDTO dto = Manager.upsertStreamMapping(request);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in upserting mapping in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Retrieve Mapping from DB
	 * 
	 * @param eventStreamName EventStreamName
	 * @return Successful Json or Unsuccessful Message
	 */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/mapping/{eventStreamName}")
	public String findStreamMapping(@PathParam("eventStreamName") String eventStreamName) {
		// Show something
		try {
			logger.traceEntry(eventStreamName);
			if (null == eventStreamName || eventStreamName.isEmpty()) {
				logger.error("Invalid key entered");
				throw new Exception();
			}
			ContextMappingDefinitionDTO dto = Manager.findStreamMapping(eventStreamName);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in reading mapping in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Delete Mapping from DB
	 * 
	 * @param eventStreamName EventStreamName
	 * @return 1(true) or Error Message
	 */
	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/mapping/{eventStreamName}")
	public String removeStreamMapping(@PathParam("eventStreamName") String eventStreamName) {
		// Annihilate something
		try {
			logger.traceEntry(eventStreamName);
			Integer deleteStatus = Manager.removeStreamMapping(eventStreamName);
			return logger.traceExit(deleteStatus.toString());
		} catch (Exception e) {
			logger.error("Error in deleting mapping in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Upsert Granular Update to Mapping into DB
	 * 
	 * @param request Json
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/granularmapping")
	public String granularMappingUpdate(String request) {
		// Create something
		try {
			logger.traceEntry(request);
			ContextMappingDefinitionDTO dto = Manager.granularMappingUpdate(request);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in processing granular update to mapping in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Delete Granular Data from Mapping
	 * 
	 * @param request Json
	 * @return Successful Json or Unsuccessful Message
	 */
	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/granularmapping")
	public String granularMappingDelete(String request) {
		// Annihilate something
		try {
			logger.traceEntry(request);
			ContextMappingDefinitionDTO dto = Manager.granularMappingDelete(request);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in deleting granular data from mapping in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * This method with update the Redis with latest Metadata and Configurations
	 * 
	 * @return Metadata Json
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/contextexecute")
	public String executeStream() {
		try {
			logger.traceEntry();
			return logger.traceExit(Manager.executeStream());
		} catch (Exception e) {
			logger.error("Error Executing Stream in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to start the Spark execution
	 * 
	 * @return Rest call to start the spark execution
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/startsparkexecution")
	public String startSparkExecution() {
		try {
			logger.traceEntry();
			return logger.traceExit(Manager.startSparkExecution());
		} catch (Exception e) {
			logger.error("Error starting spark execution in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to stop the Spark execution
	 * 
	 * @return Rest call to stop the spark execution
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/stopsparkexecution")
	public String stopSparkExecution() {
		try {
			logger.traceEntry();
			return logger.traceExit(Manager.stopSparkExecution());
		} catch (Exception e) {
			logger.error("Error stopping spark execution in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Upsert the Input/Output Connection Configuration Detail
	 * 
	 * @param request Json
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/configurationdetails")
	public String upsertConfDetails(String request) {
		try {
			logger.traceEntry(request);
			ConfigurationDetailsDTO dto = Manager.upsertConfDetails(request);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error inserting connection details in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Retrieve the Input/Output Connection Configuration Detail
	 * 
	 * @param connName Connection Name
	 * @return Successful Json or Unsuccessful Message
	 */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/configurationdetails/{connName}")
	public String findConfDetails(@PathParam("connName") String connName) {
		try {
			logger.traceEntry(connName);
			if (null == connName || connName.isEmpty()) {
				logger.error("Invalid key entered");
				throw new Exception();
			}
			ConfigurationDetailsDTO dto = Manager.findConfDetails(connName);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error upserting connection details in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Delete the Input/Output Connection Configuration Detail
	 * 
	 * @param connName Connection Name
	 * @return 1(true) or Error Message
	 */
	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/configurationdetails/{connName}")
	public String removeConfDetails(@PathParam("connName") String connName) {
		try {
			logger.traceEntry(connName);
			Integer deleteStatus = Manager.removeConfDetails(connName);
			return logger.traceExit(deleteStatus.toString());
		} catch (Exception e) {
			logger.error("Error deleting connection details in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to start the configured Input Connection
	 * 
	 * @param inputConnName Input Connection Name
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/startinput")
	public String startInput(String inputConnName) {
		try {
			logger.traceEntry(inputConnName);
			return logger.traceExit(Manager.startInputOperation(inputConnName));
		} catch (Exception e) {
			logger.error("Error starting Input in API", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to start the configured Output Connection
	 * 
	 * @param outputConnName Output Connection Name
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/startoutput")
	public String startOutput(String outputConnName) {
		try {
			logger.traceEntry(outputConnName);
			return logger.traceExit(Manager.startOutputOperation(outputConnName));
		} catch (Exception e) {
			logger.error("Error starting Output in API", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to stop the configured Input Connection
	 * 
	 * @param inputConnName Input Connection Name
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/stopinput")
	public String stopInput(String inputConnName) {
		try {
			logger.traceEntry(inputConnName);
			return logger.traceExit(Manager.stopInputOperation(inputConnName));
		} catch (Exception e) {
			logger.error("Error stopping Input in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to stop the configured Output Connection
	 * 
	 * @param outputConnName Output Connection Name
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/stopoutput")
	public String stopOutput(String outputConnName) {
		try {
			logger.traceEntry(outputConnName);
			return logger.traceExit(Manager.stopOutputOperation(outputConnName));
		} catch (Exception e) {
			logger.error("Error stopping Output in API", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Upsert the Trap Action Configuration
	 * 
	 * @param request Json
	 * @return Successful Json or Unsuccessful Message
	 */
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/trapaction")
	public String upsertTrapAction(String request) {
		try {
			logger.traceEntry(request);
			TrapDTO dto = Manager.upsertTrapAction(request);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in upserting trap action in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Retrieve the Trap Action Configuration
	 * 
	 * @param trapActionType ip/op
	 * @param trapActionPoint Connection Name
	 * @return Successful Json or Unsuccessful Message
	 */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/trapaction/{trapActionType}/{trapActionPoint}")
	public String findTrapAction(@PathParam("trapActionType") String trapActionType,
			@PathParam("trapActionPoint") String trapActionPoint) {
		try {
			logger.traceEntry(trapActionType, trapActionPoint);
			if (null == trapActionType || trapActionType.isEmpty() || null == trapActionPoint
					|| trapActionPoint.isEmpty()) {
				logger.error("Invalid keys entered");
				throw new Exception();
			}
			List<TrapDTO> dto = Manager.findTrapAction(trapActionType, trapActionPoint);
			return logger.traceExit(gson.toJson(dto));
		} catch (Exception e) {
			logger.error("Error in reading trap action in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

	/**
	 * Method to Delete the Trap Action Configuration
	 * 
	 * @param trapActionType ip/op
	 * @param trapActionPoint Connection Name
	 * @return 1(true) or Error Message
	 */
	@DELETE
	@Produces(MediaType.TEXT_PLAIN)
	@Path("/trapaction/{trapActionType}/{trapActionPoint}")
	public String removeTrapAction(@PathParam("trapActionType") String trapActionType,
			@PathParam("trapActionPoint") String trapActionPoint) {
		try {
			logger.traceEntry(trapActionType, trapActionPoint);
			Integer deleteStatus = Manager.removeTrapAction(trapActionType, trapActionPoint);
			return logger.traceExit(deleteStatus.toString());
		} catch (Exception e) {
			logger.error("Error in deleting trap action in API - ", e);
			return logger.traceExit(gson.toJson(e.getMessage()));
		}
	}

}
