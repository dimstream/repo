package com.vmware.common.trap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.dao.Action;
import com.vmware.common.dao.ComputedStates;
import com.vmware.common.dao.ConfigurationDetails;
import com.vmware.common.dao.ConfigurationProperties;
import com.vmware.common.dao.ContextMapping;
import com.vmware.common.dao.EventStream;
import com.vmware.common.dao.ExternalLookUp;
import com.vmware.common.dao.Identifier;
import com.vmware.common.dao.PersistedFields;
import com.vmware.common.dao.ProcessContext;
import com.vmware.common.dao.RevisedFields;
import com.vmware.common.dao.Trap;
import com.vmware.common.dao.TrapActionConfig;
import com.vmware.common.dao.URLTag;
import com.vmware.common.dto.ActionDTO;
import com.vmware.common.dto.ComputedStateDTO;
import com.vmware.common.dto.ConfigurationDTO;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ConfigurationPropertiesDTO;
import com.vmware.common.dto.ContextMappingDTO;
import com.vmware.common.dto.ContextMappingDefinitionDTO;
import com.vmware.common.dto.EventStreamDTO;
import com.vmware.common.dto.ExternalLookUpDTO;
import com.vmware.common.dto.IdentifierDTO;
import com.vmware.common.dto.PersistedFieldDTO;
import com.vmware.common.dto.ProcessContextDTO;
import com.vmware.common.dto.RevisedFieldDTO;
import com.vmware.common.dto.TrapDTO;
import com.vmware.common.dto.URLTagDTO;

/**
 * Class to convert database entity to DTO and vice-versa.
 * 
 * @author vedanthr
 *
 */
public final class ObjectConverter {

	private static final Logger logger = LogManager.getLogger(ObjectConverter.class);

	public EventStream convert(EventStreamDTO dtoObject) throws SQLException {

		logger.traceEntry();
		EventStream daoObject = new EventStream();
		if (dtoObject.eventStreamName != null) {
			daoObject.eventStreamName = dtoObject.eventStreamName;
			daoObject.filter = dtoObject.filter;
			daoObject.lateJoinCondition = dtoObject.lateJoinCondition;
			if(dtoObject.evictionTime!=null)
				daoObject.evictionTime = dtoObject.evictionTime;
		} else {
			throw new IllegalStateException("DTO with blank id cannot be processed");
		}
		return logger.traceExit(daoObject);
	}

	public EventStreamDTO convert(EventStream daoObject) {

		logger.traceEntry();
		EventStreamDTO dtoObject = new EventStreamDTO();
		if (daoObject != null) {
			dtoObject.eventStreamName = daoObject.eventStreamName;
			dtoObject.filter = daoObject.filter;
			dtoObject.evictionTime = daoObject.evictionTime;
			dtoObject.lateJoinCondition = daoObject.lateJoinCondition;
			dtoObject.identifier = new ArrayList<IdentifierDTO>();
			for (Identifier identifier : daoObject.identifier) {
				dtoObject.identifier.add(convert(identifier));
			}
		}
		return logger.traceExit(dtoObject);
	}

	public IdentifierDTO convert(Identifier daoObject) {

		logger.traceEntry();
		IdentifierDTO dtoObject = new IdentifierDTO();
		if (daoObject != null) {
			dtoObject.field = daoObject.field;
		}
		return logger.traceExit(dtoObject);
	}

	public Identifier convert(IdentifierDTO dtoObject) {

		logger.traceEntry();
		Identifier daoObject = new Identifier();
		if (dtoObject != null) {
			if (dtoObject.identifierId != null) {
				daoObject.identifierId = dtoObject.identifierId;
			}
			daoObject.field = dtoObject.field;
		}
		return logger.traceExit(daoObject);
	}

	public ProcessContext convert(ProcessContextDTO dtoObject) {

		logger.traceEntry();
		ProcessContext daoObject = new ProcessContext();
		if (dtoObject != null) {
			daoObject.processContextName = dtoObject.processContextName;
			daoObject.enrichmentCompletion = dtoObject.enrichmentCompletion;
			daoObject.evictionTime = dtoObject.evictionTime;
			daoObject.singleStream = dtoObject.singleStream;
		}
		return logger.traceExit(daoObject);
	}

	public ProcessContextDTO convert(ProcessContext daoObject) {

		logger.traceEntry();
		ProcessContextDTO dtoObject = new ProcessContextDTO();
		if (daoObject != null) {
			dtoObject.processContextName = daoObject.processContextName;
			dtoObject.enrichmentCompletion = daoObject.enrichmentCompletion;
			dtoObject.evictionTime = daoObject.evictionTime;
			dtoObject.singleStream = daoObject.singleStream;
		}
		return logger.traceExit(dtoObject);
	}

	/* Test */
	public List<ContextMapping> convert(ContextMappingDefinitionDTO dtoObject) throws SQLException {

		logger.traceEntry();
		List<ContextMapping> out = new ArrayList<ContextMapping>();
		for (ContextMappingDTO mapping : dtoObject.contextMappings) {
			ContextMapping elem = new ContextMapping();
			elem.stream = convert(dtoObject.stream);
			elem.context = convert(mapping.context);
			elem.joinConditions = mapping.joinConditions;
			elem.primaryStream = mapping.primaryStream;
			out.add(elem);
		}
		return logger.traceExit(out);
	}

	public ContextMappingDefinitionDTO convert(List<ContextMapping> mappings) {

		logger.traceEntry();
		ContextMappingDefinitionDTO dtoObject = new ContextMappingDefinitionDTO();
		List<ContextMappingDTO> contextMappings = new ArrayList<ContextMappingDTO>();
		dtoObject.stream = convert(mappings.get(0).stream);
		for (ContextMapping mapping : mappings) {
			ContextMappingDTO dto = convert(mapping);
			contextMappings.add(dto);
		}
		dtoObject.contextMappings = contextMappings;
		return logger.traceExit(dtoObject);
	}

	public ComputedStates convert(ComputedStateDTO dtoObject) {

		logger.traceEntry();
		ComputedStates computedState = new ComputedStates();
		if (dtoObject != null) {
			computedState.conditions = dtoObject.conditions;
			computedState.stateName = dtoObject.stateName;

		}
		return logger.traceExit(computedState);
	}

	public ComputedStateDTO convert(ComputedStates daoObject) {

		logger.traceEntry();
		ComputedStateDTO dtoObject = new ComputedStateDTO();
		if (daoObject != null) {
			dtoObject.conditions = daoObject.conditions;
			dtoObject.stateName = daoObject.stateName;
		}
		return logger.traceExit(dtoObject);
	}

	public PersistedFields convert(PersistedFieldDTO dtoObject) {

		logger.traceEntry();
		PersistedFields daoObject = new PersistedFields();
		if (dtoObject != null) {
			daoObject.streamFieldName = dtoObject.streamFieldName;
			daoObject.contextFieldName = dtoObject.contextFieldName;
		}
		return logger.traceExit(daoObject);
	}

	public PersistedFieldDTO convert(PersistedFields daoObject) {

		logger.traceEntry();
		PersistedFieldDTO dtoObject = new PersistedFieldDTO();
		if (daoObject != null) {
			dtoObject.contextFieldName = daoObject.contextFieldName;
			dtoObject.streamFieldName = daoObject.streamFieldName;
		}
		return logger.traceExit(dtoObject);
	}

	public RevisedFields convert(RevisedFieldDTO dtoObject) {

		logger.traceEntry();
		RevisedFields daoObject = new RevisedFields();
		if (dtoObject != null) {
			daoObject.fieldName = dtoObject.fieldName;
			daoObject.expression = dtoObject.expression;
		}
		return logger.traceExit(daoObject);
	}

	public RevisedFieldDTO convert(RevisedFields daoObject) {

		logger.traceEntry();
		RevisedFieldDTO dtoObject = new RevisedFieldDTO();
		if (daoObject != null) {
			dtoObject.fieldName = daoObject.fieldName;
			dtoObject.expression = daoObject.expression;
		}
		return logger.traceExit(dtoObject);
	}
	
	public ExternalLookUp convert(ExternalLookUpDTO dtoObject) {

		logger.traceEntry();
		ExternalLookUp daoObject = null;
		if (dtoObject != null) {
			daoObject = new ExternalLookUp();
			daoObject.loginURL = dtoObject.loginURL;
			daoObject.authType = dtoObject.authType;
			daoObject.contentTypeHeader = dtoObject.contentTypeHeader;
			daoObject.methodType = dtoObject.methodType;
			daoObject.tokenKey = dtoObject.tokenKey;
		}
		return logger.traceExit(daoObject);
	}

	public ExternalLookUpDTO convert(ExternalLookUp daoObject) {

		logger.traceEntry();
		ExternalLookUpDTO dtoObject = new ExternalLookUpDTO();
		if (daoObject != null) {
			dtoObject.loginURL = daoObject.loginURL;
			dtoObject.authType = daoObject.authType;
			dtoObject.contentTypeHeader = daoObject.contentTypeHeader;
			dtoObject.methodType = daoObject.methodType;
			dtoObject.tokenKey = daoObject.tokenKey;

			dtoObject.urlTag = new ArrayList<URLTagDTO>();
			for (URLTag urlTag : daoObject.urlTag) {
				dtoObject.urlTag.add(convert(urlTag));
			}
		}
		return logger.traceExit(dtoObject);
	}

	public URLTag convert(URLTagDTO dtoObject) {

		logger.traceEntry();
		URLTag daoObject = new URLTag();
		if (dtoObject != null) {
			if (dtoObject.urlTagId != null) {
				daoObject.urlTagId = dtoObject.urlTagId;
			}
			daoObject.authorizationHeader = dtoObject.authorizationHeader;
			daoObject.contentTypeHeader = dtoObject.contentTypeHeader;
			daoObject.methodType = dtoObject.methodType;
			daoObject.queryParam = dtoObject.queryParam;
			daoObject.tag = dtoObject.tag;
			daoObject.url = dtoObject.url;
		}
		return logger.traceExit(daoObject);
	}

	public URLTagDTO convert(URLTag daoObject) {

		logger.traceEntry();
		URLTagDTO dtoObject = new URLTagDTO();
		if (daoObject != null) {
			dtoObject.authorizationHeader = daoObject.authorizationHeader;
			dtoObject.contentTypeHeader = daoObject.contentTypeHeader;
			dtoObject.methodType = daoObject.methodType;
			dtoObject.queryParam = daoObject.queryParam;
			dtoObject.tag = daoObject.tag;
			dtoObject.url = daoObject.url;
		}
		return logger.traceExit(dtoObject);
	}

	public ContextMappingDTO convert(ContextMapping daoObject) {

		logger.traceEntry();
		ContextMappingDTO dtoObject = new ContextMappingDTO();
		dtoObject.computedStates = new ArrayList<ComputedStateDTO>();
		dtoObject.persistedFields = new ArrayList<PersistedFieldDTO>();
		dtoObject.revisedFields = new ArrayList<RevisedFieldDTO>();
		
		if (daoObject != null) {
			if (daoObject.contextMappingId != null) {
				dtoObject.contextMappingId = daoObject.contextMappingId;
			}
			dtoObject.context = convert(daoObject.context);
			dtoObject.joinConditions = daoObject.joinConditions;
			dtoObject.primaryStream = daoObject.primaryStream;
			dtoObject.nestedField = daoObject.nestedField;

			for (ComputedStates state : daoObject.computedStates) {
				dtoObject.computedStates.add(convert(state));
			}

			for (PersistedFields field : daoObject.persistedFields) {
				dtoObject.persistedFields.add(convert(field));
			}
			
			for (RevisedFields field : daoObject.revisedFields) {
				dtoObject.revisedFields.add(convert(field));
			}

			dtoObject.externalLookUp = convert(daoObject.externalLookUp);
		}
		return logger.traceExit(dtoObject);
	}

	public ContextMapping convert(ContextMappingDTO dtoObject) throws SQLException {

		logger.traceEntry();
		ContextMapping daoObject = new ContextMapping();
		if (dtoObject != null) {
			if (dtoObject.contextMappingId != null) {
				daoObject.contextMappingId = dtoObject.contextMappingId;
			}
			daoObject.context = convert(dtoObject.context);
			daoObject.joinConditions = dtoObject.joinConditions;
			daoObject.primaryStream = dtoObject.primaryStream;
			daoObject.nestedField = dtoObject.nestedField;
			daoObject.stream = convert(dtoObject.stream);
		}
		return logger.traceExit(daoObject);
	}

	public ConfigurationDetails convert(ConfigurationDetailsDTO dtoObject) throws SQLException {

		logger.traceEntry();
		ConfigurationDetails daoObject = new ConfigurationDetails();
		if (dtoObject.configurationName != null) {
			daoObject.configurationName = dtoObject.configurationName;
			daoObject.configurationType = dtoObject.configurationType;
			daoObject.configurationFrequency = dtoObject.configurationFrequency;
			daoObject.configurationFlow = dtoObject.configurationFlow;
			if (dtoObject.streamName != null)
				daoObject.streamName = dtoObject.streamName;
		} else {
			throw new IllegalStateException("DTO with blank ConfigurationName cannot be processed");
		}
		return logger.traceExit(daoObject);
	}

	public ConfigurationDetailsDTO convert(ConfigurationDetails daoObject) {

		logger.traceEntry();
		ConfigurationDetailsDTO dtoObject = new ConfigurationDetailsDTO();
		if (daoObject != null) {
			dtoObject.configurationName = daoObject.configurationName;
			dtoObject.configurationType = daoObject.configurationType;
			dtoObject.configurationFrequency = daoObject.configurationFrequency;
			dtoObject.configurationFlow = daoObject.configurationFlow;
			if (daoObject.streamName != null)
				dtoObject.streamName = daoObject.streamName;
			dtoObject.configurationProperties = new ArrayList<ConfigurationPropertiesDTO>();
			for (ConfigurationProperties configurationProperties : daoObject.configurationProperties) {
				dtoObject.configurationProperties.add(convert(configurationProperties));
			}
		}
		return logger.traceExit(dtoObject);
	}

	public ConfigurationProperties convert(ConfigurationPropertiesDTO dtoObject) {

		logger.traceEntry();
		ConfigurationProperties daoObject = new ConfigurationProperties();
		if (dtoObject != null) {
			if (dtoObject.configurationId != null) {
				daoObject.configurationId = dtoObject.configurationId;
			}
			daoObject.configurationKey = dtoObject.configurationKey;
			daoObject.configurationValue = dtoObject.configurationValue;
		}
		return logger.traceExit(daoObject);
	}

	public ConfigurationPropertiesDTO convert(ConfigurationProperties daoObject) {

		logger.traceEntry();
		ConfigurationPropertiesDTO dtoObject = new ConfigurationPropertiesDTO();
		if (daoObject != null) {
			dtoObject.configurationKey = daoObject.configurationKey;
			dtoObject.configurationValue = daoObject.configurationValue;
		}
		return logger.traceExit(dtoObject);
	}

	public Trap convert(TrapDTO dtoObject) throws SQLException {

		logger.traceEntry();
		Trap daoObject = new Trap();
		if (dtoObject.pluginType != null && dtoObject.pluginPoint != null && dtoObject.condition != null) {
			if (dtoObject.trapId != null) {
				daoObject.trapId = dtoObject.trapId;
			}
			daoObject.pluginType = dtoObject.pluginType;
			daoObject.pluginPoint = dtoObject.pluginPoint;
			daoObject.condition = dtoObject.condition;
			daoObject.joinCondition = dtoObject.joinCondition;
			daoObject.contextName = dtoObject.contextName;
		} else {
			throw new IllegalStateException("DTO with blank pluginType/pluginPoint/condition cannot be processed");
		}
		return logger.traceExit(daoObject);
	}

	public Action convert(ActionDTO dtoObject) {

		logger.traceEntry();
		Action daoObject = new Action();
		if (dtoObject != null) {
			if (dtoObject.actionId != null) {
				daoObject.actionId = dtoObject.actionId;
			}
			daoObject.actionType = dtoObject.actionType;
		}
		return logger.traceExit(daoObject);
	}

	public TrapActionConfig convert(ConfigurationDTO dtoObject) {

		logger.traceEntry();
		TrapActionConfig daoObject = new TrapActionConfig();
		if (dtoObject != null) {
			if (dtoObject.configId != null) {
				daoObject.configId = dtoObject.configId;
			}
			daoObject.key = dtoObject.key;
			daoObject.value = dtoObject.value;
		}
		return logger.traceExit(daoObject);
	}

	public TrapDTO convert(Trap daoObject) {

		logger.traceEntry();
		TrapDTO dtoObject = new TrapDTO();
		if (daoObject != null) {
			dtoObject.trapId = daoObject.trapId;
			dtoObject.pluginType = daoObject.pluginType;
			dtoObject.pluginPoint = daoObject.pluginPoint;
			dtoObject.condition = daoObject.condition;
			dtoObject.joinCondition = daoObject.joinCondition;
			dtoObject.contextName = daoObject.contextName;
			dtoObject.actions = new ArrayList<ActionDTO>();
			for (Action action : daoObject.actions) {
				dtoObject.actions.add(convert(action));
			}
		}
		return logger.traceExit(dtoObject);
	}

	public ActionDTO convert(Action daoObject) {

		logger.traceEntry();
		ActionDTO dtoObject = new ActionDTO();
		if (daoObject != null) {
			dtoObject.actionId = daoObject.actionId;
			dtoObject.actionType = daoObject.actionType;
			dtoObject.configuration = new ArrayList<ConfigurationDTO>();
			for (TrapActionConfig trapActionConfig : daoObject.configuration) {
				dtoObject.configuration.add(convert(trapActionConfig));
			}
		}
		return logger.traceExit(dtoObject);
	}

	public ConfigurationDTO convert(TrapActionConfig daoObject) {

		logger.traceEntry();
		ConfigurationDTO dtoObject = new ConfigurationDTO();
		if (daoObject != null) {
			dtoObject.configId = daoObject.configId;
			dtoObject.key = daoObject.key;
			dtoObject.value = daoObject.value;
		}
		return logger.traceExit(dtoObject);
	}

	public List<TrapDTO> convertTrapList(List<Trap> daoObject) {

		logger.traceEntry();
		List<TrapDTO> dtoObjects = new ArrayList<TrapDTO>();
		for (Trap trap : daoObject) {
			TrapDTO dtoObject = new TrapDTO();
			dtoObject.trapId = trap.trapId;
			dtoObject.pluginType = trap.pluginType;
			dtoObject.pluginPoint = trap.pluginPoint;
			dtoObject.condition = trap.condition;
			dtoObject.joinCondition = trap.joinCondition;
			dtoObject.contextName = trap.contextName;
			dtoObject.actions = new ArrayList<ActionDTO>();
			for (Action action : trap.actions) {
				dtoObject.actions.add(convert(action));
			}
			dtoObjects.add(dtoObject);
		}
		return logger.traceExit(dtoObjects);
	}
}