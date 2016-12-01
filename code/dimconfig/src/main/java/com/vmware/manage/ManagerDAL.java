package com.vmware.manage;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.stmt.QueryBuilder;
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
import com.vmware.common.dto.ContextMappingOutputDTO;
import com.vmware.common.dto.EventStreamDTO;
import com.vmware.common.dto.GlobalConfigurationDTO;
import com.vmware.common.dto.IdentifierDTO;
import com.vmware.common.dto.PersistedFieldDTO;
import com.vmware.common.dto.ProcessContextDTO;
import com.vmware.common.dto.RevisedFieldDTO;
import com.vmware.common.dto.StreamConfigurationDTO;
import com.vmware.common.dto.TrapDTO;
import com.vmware.common.dto.URLTagDTO;
import com.vmware.common.trap.ObjectConverter;
import com.vmware.conn.DBConnection;

/**
 * Class to Manage all the database operations(CRUD) specified by Manager API
 * 
 * @author ghimanshu
 *
 */
public class ManagerDAL {

	private static final Logger logger = LogManager.getLogger(ManagerDAL.class);

	/**
	 * Database Operation to Create Event Stream
	 * 
	 * @param dto EventStreamDTO
	 * @return EventStream
	 * @throws SQLException SQLException
	 */
	public EventStream create(EventStreamDTO dto) throws SQLException {

		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();

		ProcessContext evDetails = readProcessContext(dto.eventStreamName.trim());
		if (evDetails != null) {
			logger.error("Process Context already exists with the same name.");
			throw new SQLException("Process Context already exists with the same name.");
		}

		EventStream eventStreamDaoObj = transformer.convert(dto);
		Dao<EventStream, String> eventStreamDao = DaoManager.createDao(DBConnection.getConnection(), EventStream.class);
		eventStreamDao.assignEmptyForeignCollection(eventStreamDaoObj, "identifier");
		for (IdentifierDTO idDto : dto.identifier) {
			eventStreamDaoObj.identifier.add(transformer.convert(idDto));
		}
		eventStreamDaoObj = eventStreamDao.createIfNotExists(eventStreamDaoObj);
		eventStreamDao.refresh(eventStreamDaoObj);
		return logger.traceExit(eventStreamDaoObj);
	}

	/**
	 * Database Operation to Create Context
	 * 
	 * @param dto ProcessContextDTO
	 * @return ProcessContext
	 * @throws SQLException SQLException
	 */
	public ProcessContext create(ProcessContextDTO dto) throws SQLException {

		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();

		EventStream evDetails = readEventStream(dto.processContextName.trim());
		if (evDetails != null) {
			logger.error("Event Stream already exists with the same name.");
			throw new SQLException("Event Stream already exists with the same name.");
		}

		ProcessContext daoObj = transformer.convert(dto);
		Dao<ProcessContext, String> dao = DaoManager.createDao(DBConnection.getConnection(), ProcessContext.class);
		dao.createIfNotExists(daoObj);
		return logger.traceExit(daoObj);
	}

	/**
	 * Database Operation to Create Mapping
	 * 
	 * @param dto ContextMappingDTO
	 * @throws SQLException SQLException
	 */
	public void create(ContextMappingDTO dto) throws SQLException {

		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();
		ContextMapping mapping = transformer.convert(dto);
		EventStream evDetails = readEventStream(transformer.convert(dto.stream).eventStreamName.trim());
		ProcessContext contextDetails = readProcessContext(transformer.convert(dto.context).processContextName.trim());

		if (evDetails != null && contextDetails != null) {
			mapping.stream = evDetails;
			mapping.context = contextDetails;

			mapping.externalLookUp = transformer.convert(dto.externalLookUp);
			if (mapping.externalLookUp != null) {
				Dao<ExternalLookUp, Integer> dao1 = DaoManager.createDao(DBConnection.getConnection(),
						ExternalLookUp.class);
				dao1.assignEmptyForeignCollection(mapping.externalLookUp, "urlTag");
				mapping.externalLookUp = dao1.createIfNotExists(mapping.externalLookUp);

				if (dto.externalLookUp != null) {
					for (URLTagDTO urlTag : dto.externalLookUp.urlTag) {
						URLTag urlTag2 = transformer.convert(urlTag);
						mapping.externalLookUp.urlTag.add(urlTag2);
						urlTag2.externalLookUp = mapping.externalLookUp;
					}
				}

				dao1.refresh(mapping.externalLookUp);
			}

			Dao<ContextMapping, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), ContextMapping.class);
			dao.assignEmptyForeignCollection(mapping, "persistedFields");
			dao.assignEmptyForeignCollection(mapping, "revisedFields");
			dao.assignEmptyForeignCollection(mapping, "computedStates");
			dao.createIfNotExists(mapping);

			if(dto.computedStates != null){
				for (ComputedStateDTO state : dto.computedStates) {
					mapping.computedStates.add(transformer.convert(state));
				}	
			}
			
			if(dto.persistedFields != null){
				for (PersistedFieldDTO field : dto.persistedFields) {
					mapping.persistedFields.add(transformer.convert(field));
				}	
			}

			if(dto.revisedFields != null){
				for (RevisedFieldDTO field : dto.revisedFields) {
					mapping.revisedFields.add(transformer.convert(field));
				}	
			}
			dao.refresh(mapping);
		} else {
			throw new SQLException("Event stream does not exist");
		}
		logger.traceExit();
	}

	/**
	 * Database Operation to Create ComputedStates of Mapping
	 * 
	 * @param newComputedStates ComputedStates
	 * @return ComputedStates
	 * @throws SQLException SQLException
	 */
	public ComputedStates create(ComputedStates newComputedStates) throws SQLException {

		logger.traceEntry();
		Dao<ComputedStates, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), ComputedStates.class);
		dao.create(newComputedStates);
		return logger.traceExit(newComputedStates);
	}

	/**
	 * Database Operation to Create PersistedFields of Mapping
	 * 
	 * @param newPersistedFields PersistedFields
	 * @return PersistedFields
	 * @throws SQLException SQLException
	 */
	public PersistedFields create(PersistedFields newPersistedFields) throws SQLException {

		logger.traceEntry();
		Dao<PersistedFields, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), PersistedFields.class);
		dao.create(newPersistedFields);
		return logger.traceExit(newPersistedFields);
	}
	
	/**
	 * Database Operation to Create RevisedFields of Mapping
	 * 
	 * @param newRevisedFields RevisedFields
	 * @return RevisedFields
	 * @throws SQLException SQLException
	 */
	public RevisedFields create(RevisedFields newRevisedFields) throws SQLException {

		logger.traceEntry();
		Dao<RevisedFields, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), RevisedFields.class);
		dao.create(newRevisedFields);
		return logger.traceExit(newRevisedFields);
	}

	/**
	 * Database Operation to Create Identifier of Event Stream
	 * 
	 * @param newIdentifier Identifier
	 * @return Identifier
	 * @throws SQLException SQLException
	 */
	public Identifier create(Identifier newIdentifier) throws SQLException {

		logger.traceEntry();
		Dao<Identifier, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), Identifier.class);
		dao.create(newIdentifier);
		return logger.traceExit(newIdentifier);
	}

	/**
	 * Database Operation to Create Trap
	 * 
	 * @param dto TrapDTO
	 * @return Trap
	 * @throws SQLException SQLException
	 */
	public Trap create(TrapDTO dto) throws SQLException {

		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();

		Trap trap = transformer.convert(dto);
		Dao<Trap, String> trapDao = DaoManager.createDao(DBConnection.getConnection(), Trap.class);
		trap = trapDao.createIfNotExists(trap);
		trapDao.refresh(trap);

		trapDao.assignEmptyForeignCollection(trap, "actions");
		for (ActionDTO actDto : dto.actions) {
			Action action = transformer.convert(actDto);
			trap.actions.add(action);
			Dao<Action, String> actionDao = DaoManager.createDao(DBConnection.getConnection(), Action.class);
			actionDao.assignEmptyForeignCollection(action, "configuration");
			for (ConfigurationDTO taConfig : actDto.configuration)
				action.configuration.add(transformer.convert(taConfig));
		}
		trapDao.refresh(trap);
		return logger.traceExit(trap);
	}

	/**
	 * Database Operation to Create Event Stream
	 * 
	 * @param dtoObj EventStreamDTO
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public Integer update(EventStreamDTO dtoObj) throws SQLException {

		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();
		EventStream daoObj = transformer.convert(dtoObj);
		List<IdentifierDTO> dtoList = new ArrayList<>();
		Integer output = 0;

		Dao<EventStream, String> dao = DaoManager.createDao(DBConnection.getConnection(), EventStream.class);
		dao.assignEmptyForeignCollection(daoObj, "identifier");

		EventStream perObj = dao.queryForId(dtoObj.eventStreamName);

		Iterator<Identifier> iterator = perObj.identifier.iterator();
		Integer counter = 0;

		while (iterator.hasNext()) {
			IdentifierDTO dto = dtoObj.identifier.get(counter);
			dto.identifierId = iterator.next().identifierId;
			dtoList.add(dto);
			counter++;
		}
		dtoObj.identifier = dtoList;

		for (IdentifierDTO idDto : dtoObj.identifier) {
			Identifier identifier = transformer.convert(idDto);
			identifier.eventStream = perObj;
			daoObj.identifier.update(identifier);
		}
		output = dao.update(daoObj);

		return logger.traceExit(output);
	}

	/**
	 * Database Operation to Update Context
	 * 
	 * @param dtoObj ProcessContextDTO
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public Integer update(ProcessContextDTO dtoObj) throws SQLException {

		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();
		ProcessContext daoObj = transformer.convert(dtoObj);
		Integer output = 0;

		Dao<ProcessContext, String> dao = DaoManager.createDao(DBConnection.getConnection(), ProcessContext.class);
		output = dao.update(daoObj);

		return logger.traceExit(output);
	}
	
	/**
	 * Database Operation to Update Context
	 * 
	 * @param dto ContextMappingDTO
	 * @param streamName Event Stream Name
	 * @throws SQLException SQLException
	 */
	public void update(ContextMappingDTO dto, String streamName)throws SQLException {
		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();
		EventStream evDetails = readEventStream(streamName);
		dto.stream = transformer.convert(evDetails);
		Dao<ContextMapping, String> dao = DaoManager.createDao(DBConnection.getConnection(), ContextMapping.class);
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("Stream_id", streamName);
		map.put("Context_id", dto.context.processContextName);
		List<ContextMapping> contextMappingDao = dao.queryForFieldValues(map);
		
		boolean flag = false;
		if(dto.primaryStream == true){
			contextMappingDao.get(0).primaryStream = dto.primaryStream;
			if(dto.joinConditions!=null && !dto.joinConditions.isEmpty())
				contextMappingDao.get(0).joinConditions = dto.joinConditions;
		}
		else if (dto.primaryStream == false){
			if(dto.joinConditions!=null && !dto.joinConditions.isEmpty()){
				contextMappingDao.get(0).joinConditions = dto.joinConditions;
				flag = true;
			}
			else if(contextMappingDao.get(0).joinConditions != null && !contextMappingDao.get(0).joinConditions.isEmpty()){
				flag = true;
			}
			if(flag)
				contextMappingDao.get(0).primaryStream = dto.primaryStream;
			else
				throw new SQLException("Cannot declare primary stream as false, without join condition");
		}
		
		Dao<PersistedFields, Integer> persistedFieldsDao = DaoManager.createDao(DBConnection.getConnection(),
				PersistedFields.class);
		dao.assignEmptyForeignCollection(contextMappingDao.get(0), "persistedFields");
		for(PersistedFieldDTO persistedFieldsDto: dto.persistedFields){
			for(PersistedFields persistedFields: contextMappingDao.get(0).persistedFields){
				if(persistedFieldsDto.contextFieldName.equals(persistedFields.contextFieldName)){
					//delete persisted field	
					persistedFieldsDao.delete(persistedFields);
					break;
				}
			}
		}
		for(PersistedFieldDTO persistedFieldsDto: dto.persistedFields){
			contextMappingDao.get(0).persistedFields.add(transformer.convert(persistedFieldsDto));
		}
		
		Dao<ComputedStates, Integer> computedStatesDao = DaoManager.createDao(DBConnection.getConnection(),
				ComputedStates.class);
		dao.assignEmptyForeignCollection(contextMappingDao.get(0), "computedStates");
		for(ComputedStateDTO computedStatesDto: dto.computedStates){
			for(ComputedStates computedStates: contextMappingDao.get(0).computedStates){
				if(computedStatesDto.stateName.equals(computedStates.stateName)){
					//delete persisted field	
					computedStatesDao.delete(computedStates);
					break;
				}
			}
		}
		for(ComputedStateDTO computedStateDTO: dto.computedStates){
			contextMappingDao.get(0).computedStates.add(transformer.convert(computedStateDTO));
		}
		
		Dao<RevisedFields, Integer> revisedFieldsDao = DaoManager.createDao(DBConnection.getConnection(),
				RevisedFields.class);
		dao.assignEmptyForeignCollection(contextMappingDao.get(0), "revisedFields");
		for(RevisedFieldDTO revisedFieldDto: dto.revisedFields){
			for(RevisedFields revisedFields: contextMappingDao.get(0).revisedFields){
				if(revisedFieldDto.fieldName.equals(revisedFields.fieldName)){
					//delete persisted field	
					revisedFieldsDao.delete(revisedFields);
					break;
				}
			}
		}
		
		for(RevisedFieldDTO revisedFieldDto: dto.revisedFields){
			contextMappingDao.get(0).revisedFields.add(transformer.convert(revisedFieldDto));
		}
		
		dao.update(contextMappingDao.get(0));
		dao.refresh(contextMappingDao.get(0));
		
	}
	
	/**
	 * Database Operation to Delete Mapping
	 * 
	 * @param dto ContextMappingDTO
	 * @param streamName Event Stream Name
	 * @throws SQLException SQLException
	 */
	public void delete(ContextMappingDTO dto, String streamName)throws SQLException {
		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();
		EventStream evDetails = readEventStream(streamName);
		dto.stream = transformer.convert(evDetails);
		Dao<ContextMapping, String> dao = DaoManager.createDao(DBConnection.getConnection(), ContextMapping.class);
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("Stream_id", streamName);
		map.put("Context_id", dto.context.processContextName);
		List<ContextMapping> contextMappingDao = dao.queryForFieldValues(map);
		
		Dao<PersistedFields, Integer> persistedFieldsDao = DaoManager.createDao(DBConnection.getConnection(),
				PersistedFields.class);
		dao.assignEmptyForeignCollection(contextMappingDao.get(0), "persistedFields");
		for(PersistedFieldDTO persistedFieldsDto: dto.persistedFields){
			for(PersistedFields persistedFields: contextMappingDao.get(0).persistedFields){
				if(persistedFieldsDto.contextFieldName.equals(persistedFields.contextFieldName)){
					//delete persisted field	
					persistedFieldsDao.delete(persistedFields);
					break;
				}
			}
		}
		
		Dao<ComputedStates, Integer> computedStatesDao = DaoManager.createDao(DBConnection.getConnection(),
				ComputedStates.class);
		dao.assignEmptyForeignCollection(contextMappingDao.get(0), "computedStates");
		for(ComputedStateDTO computedStatesDto: dto.computedStates){
			for(ComputedStates computedStates: contextMappingDao.get(0).computedStates){
				if(computedStatesDto.stateName.equals(computedStates.stateName)){
					//delete persisted field	
					computedStatesDao.delete(computedStates);
					break;
				}
			}
		}
		
		Dao<RevisedFields, Integer> revisedFieldsDao = DaoManager.createDao(DBConnection.getConnection(),
				RevisedFields.class);
		dao.assignEmptyForeignCollection(contextMappingDao.get(0), "revisedFields");
		for(RevisedFieldDTO revisedFieldDto: dto.revisedFields){
			for(RevisedFields revisedFields: contextMappingDao.get(0).revisedFields){
				if(revisedFieldDto.fieldName.equals(revisedFields.fieldName)){
					//delete persisted field	
					revisedFieldsDao.delete(revisedFields);
					break;
				}
			}
		}
		
	}

	/**
	 * Database Operation to Update Identifier of Stream
	 * 
	 * @param newIdentifier Identifier
	 * @return Identifier
	 * @throws SQLException SQLException
	 */
	public Identifier update(Identifier newIdentifier) throws SQLException {

		logger.traceEntry();
		Dao<Identifier, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), Identifier.class);
		dao.update(newIdentifier);
		return logger.traceExit(newIdentifier);
	}

	/**
	 * Database Operation to Read Event
	 * 
	 * @param streamName Event Stream Name
	 * @return EventStream
	 * @throws SQLException SQLException
	 */
	public EventStream readEventStream(String streamName) throws SQLException {

		logger.traceEntry();
		EventStream eventObj = new EventStream();
		Dao<EventStream, String> dao = DaoManager.createDao(DBConnection.getConnection(), EventStream.class);
		eventObj = dao.queryForId(streamName);

		return logger.traceExit(eventObj);
	}

	/**
	 * Database Operation to Read Context
	 * 
	 * @param contextName Context Name
	 * @return ProcessContext
	 * @throws SQLException SQLException
	 */
	public ProcessContext readProcessContext(String contextName) throws SQLException {

		logger.traceEntry();
		ProcessContext persistanceObj = new ProcessContext();
		Dao<ProcessContext, String> dao = DaoManager.createDao(DBConnection.getConnection(), ProcessContext.class);
		persistanceObj = dao.queryForId(contextName);

		return logger.traceExit(persistanceObj);
	}

	/**
	 * Database Operation to Read Mapping
	 * 
	 * @param streamName Event Stream Name
	 * @return ContextMappingDefinitionDTO
	 * @throws SQLException SQLException
	 */
	public ContextMappingDefinitionDTO readContextMapping(String streamName) throws SQLException {

		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();
		ContextMappingDefinitionDTO output = new ContextMappingDefinitionDTO();
		EventStream eventDetails = readEventStream(streamName);
		if (eventDetails != null) {
			Dao<ContextMapping, String> dao = DaoManager.createDao(DBConnection.getConnection(), ContextMapping.class);
			List<ContextMapping> out = dao.queryForEq("Stream_id", streamName);

			if (out == null || out.isEmpty()) {
				return null;
			}

			for (ContextMapping map : out) {
				map.stream = eventDetails;
			}
			output = transformer.convert(out);
		} else {
			throw new SQLException("Event stream does not exist");
		}
		return logger.traceExit(output);
	}

	/**
	 * Database Operation to Read Identifier of Stream
	 * 
	 * @param identifier Identifier
	 * @return Identifier
	 * @throws SQLException SQLException
	 */
	public Identifier read(Identifier identifier) throws SQLException {

		logger.traceEntry();
		Dao<Identifier, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), Identifier.class);
		return logger.traceExit(dao.queryForId(identifier.identifierId));
	}

	/**
	 * Database Operation to Read Trap Action
	 * 
	 * @param pluginType ip/op
	 * @param pluginPoint Connection Name
	 * @param condition Join Condition
	 * @return List of Trap
	 * @throws SQLException SQLException
	 */
	public List<Trap> readTrapAction(String pluginType, String pluginPoint, String condition) throws SQLException {

		logger.traceEntry();
		Dao<Trap, String> dao = DaoManager.createDao(DBConnection.getConnection(), Trap.class);
		List<Trap> out = null;
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("pluginType", pluginType);
		map.put("pluginPoint", pluginPoint);
		if (condition != null) {
			map.put("condition", condition);
		}
		out = dao.queryForFieldValuesArgs(map);
		return logger.traceExit(out);
	}

	/**
	 * Database Operation to Delete Stream
	 * 
	 * @param streamName Event Stream Name
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public Integer deleteEventStream(String streamName) throws SQLException {

		logger.traceEntry();
		Integer status = 0;
		Dao<EventStream, String> dao = DaoManager.createDao(DBConnection.getConnection(), EventStream.class);
		EventStream eventObj = readEventStream(streamName);
		if (null == eventObj) {
			throw new SQLException("Event stream does not exist");
		}

		for (Identifier identifier : eventObj.identifier) {
			delete(identifier);
		}
		status = dao.deleteById(streamName);
		return logger.traceExit(status);
	}

	/**
	 * Database Operation to Delete Context
	 * 
	 * @param contextName Context Name
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public Integer deleteProcessContext(String contextName) throws SQLException {

		logger.traceEntry();
		Integer status = 0;
		Dao<ProcessContext, String> dao = DaoManager.createDao(DBConnection.getConnection(), ProcessContext.class);
		ProcessContext persistenceObj = readProcessContext(contextName);
		if (persistenceObj == null) {
			throw new SQLException("Process context does not exist");
		}

		status = dao.deleteById(contextName);
		return logger.traceExit(status);
	}

	/**
	 * Database Operation to Delete Mapping
	 * 
	 * @param streamName Event Stream Name
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public Integer deleteContextMapping(String streamName) throws SQLException {

		logger.traceEntry();

		Dao<ContextMapping, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), ContextMapping.class);
		Dao<ComputedStates, Integer> computedStatesDao = DaoManager.createDao(DBConnection.getConnection(),
				ComputedStates.class);
		Dao<PersistedFields, Integer> persistedFieldsDao = DaoManager.createDao(DBConnection.getConnection(),
				PersistedFields.class);
		Dao<RevisedFields, Integer> revisedFieldsDao = DaoManager.createDao(DBConnection.getConnection(),
				RevisedFields.class);
		Dao<ExternalLookUp, Integer> externalLookUpDao = DaoManager.createDao(DBConnection.getConnection(),
				ExternalLookUp.class);
		Dao<URLTag, Integer> urlTagDao = DaoManager.createDao(DBConnection.getConnection(), URLTag.class);

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("Stream_id", streamName);

		List<ContextMapping> out = dao.queryForFieldValues(map);
		for (ContextMapping contextMapping2 : out) {
			computedStatesDao.delete(contextMapping2.computedStates);
			persistedFieldsDao.delete(contextMapping2.persistedFields);
			revisedFieldsDao.delete(contextMapping2.revisedFields);
			externalLookUpDao.delete(contextMapping2.externalLookUp);
			if (contextMapping2.externalLookUp != null)
				for (URLTag urlTag : contextMapping2.externalLookUp.urlTag)
					urlTagDao.delete(urlTag);
		}
		dao.delete(out);

		return logger.traceExit(1);
	}

	/**
	 * Database Operation to Delete Trap Action
	 * 
	 * @param pluginType ip/op
	 * @param pluginPoint Connection Name
	 * @param condition Join Condition
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public Integer delete(String pluginType, String pluginPoint, String condition) throws SQLException {

		logger.traceEntry();

		Dao<Trap, String> trapDao = DaoManager.createDao(DBConnection.getConnection(), Trap.class);
		Dao<Action, Integer> actionDao = DaoManager.createDao(DBConnection.getConnection(), Action.class);
		Dao<TrapActionConfig, Integer> trapActionConfigDao = DaoManager.createDao(DBConnection.getConnection(),
				TrapActionConfig.class);

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("pluginType", pluginType);
		map.put("pluginPoint", pluginPoint);
		if (condition != null) {
			map.put("condition", condition);
		}
		List<Trap> out = trapDao.queryForFieldValuesArgs(map);

		for (Trap trap : out) {
			for (Action action : trap.actions) {
				trapActionConfigDao.delete(action.configuration);
			}
			actionDao.delete(trap.actions);
		}
		trapDao.delete(out);
		return logger.traceExit(1);
	}

	/**
	 * Database Operation to Delete Identifier of Stream
	 * 
	 * @param newIdentifier Identifier
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public int delete(Identifier newIdentifier) throws SQLException {

		logger.traceEntry();
		Dao<Identifier, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), Identifier.class);
		return logger.traceExit(dao.delete(newIdentifier));
	}

	/**
	 * Database Operation to Delete Configuration Properties of Connection
	 * 
	 * @param configurationProperties ConfigurationProperties
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public int delete(ConfigurationProperties configurationProperties) throws SQLException {

		logger.traceEntry();
		Dao<ConfigurationProperties, Integer> dao = DaoManager.createDao(DBConnection.getConnection(),
				ConfigurationProperties.class);
		return logger.traceExit(dao.delete(configurationProperties));
	}

	/**
	 * Database Operation to Delete PersistedFields of Mapping
	 * 
	 * @param newPersistedFields PersistedFields
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public int delete(PersistedFields newPersistedFields) throws SQLException {

		logger.traceEntry();
		Dao<PersistedFields, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), PersistedFields.class);
		return logger.traceExit(dao.deleteById(newPersistedFields.persistedFieldsId));
	}
	
	/**
	 * Database Operation to Delete RevisedFields of Mapping
	 * 
	 * @param newRevisedFields RevisedFields
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public int delete(RevisedFields newRevisedFields) throws SQLException {

		logger.traceEntry();
		Dao<RevisedFields, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), RevisedFields.class);
		return logger.traceExit(dao.deleteById(newRevisedFields.revisedFieldsId));
	}

	/**
	 * Database Operation to Delete ComputedStates of Mapping
	 * 
	 * @param newComputedStates ComputedStates
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public int delete(ComputedStates newComputedStates) throws SQLException {

		logger.traceEntry();
		Dao<ComputedStates, Integer> dao = DaoManager.createDao(DBConnection.getConnection(), ComputedStates.class);
		return logger.traceExit(dao.deleteById(newComputedStates.computedStatesId));
	}

	/**
	 * Database Operation to Retrieve all Output Connection Details 
	 * 
	 * @return List of Configuration Details
	 * @throws SQLException SQLException
	 */
	public List<ConfigurationDetails> generateOutputConfiguration() throws SQLException {

		logger.traceEntry();
		Dao<ConfigurationDetails, String> dao = DaoManager.createDao(DBConnection.getConnection(),
				ConfigurationDetails.class);
		QueryBuilder<ConfigurationDetails, String> queryBuilder = dao.queryBuilder();
		queryBuilder.where().eq("configurationFlow", "Output");
		List<ConfigurationDetails> configurationDetails = queryBuilder.query();

		return logger.traceExit(configurationDetails);
	}

	/**
	 * Database Operation to Retrieve all Trap Action Configuration
	 * 
	 * @return List of Trap
	 * @throws SQLException SQLException
	 */
	public List<Trap> getTrapActionConfiguration() throws SQLException {

		logger.traceEntry();
		Dao<Trap, String> dao = DaoManager.createDao(DBConnection.getConnection(), Trap.class);
		QueryBuilder<Trap, String> queryBuilder = dao.queryBuilder();
		List<Trap> trap = queryBuilder.query();

		return logger.traceExit(trap);
	}

	/**
	 * Database Operation to generate metadata by reading all configurations 
	 * 
	 * @return GlobalConfigurationDTO
	 * @throws SQLException SQLException
	 */
	public GlobalConfigurationDTO generateConfiguration() throws SQLException {

		logger.traceEntry();
		GlobalConfigurationDTO globalConfig = new GlobalConfigurationDTO();

		Dao<EventStream, String> dao = DaoManager.createDao(DBConnection.getConnection(), EventStream.class);
		List<EventStream> allEvents = dao.queryForAll();
		List<ContextMappingOutputDTO> outputElem = null;
		List<StreamConfigurationDTO> output = new ArrayList<>();

		for (EventStream event : allEvents) {
			ContextMappingDefinitionDTO dataObj = new ManagerDAL().readContextMapping(event.eventStreamName);
			ObjectConverter transformer = new ObjectConverter();
			StreamConfigurationDTO streamConfig = new StreamConfigurationDTO();
			
			if (dataObj == null){
				streamConfig.stream = transformer.convert(event);
				output.add(streamConfig);
				continue;				
			}
			
			streamConfig.stream = dataObj.stream;
			outputElem = new ArrayList<>();

			for (ContextMappingDTO mapping : dataObj.contextMappings) {
				ContextMappingOutputDTO contextMapping = new ContextMappingOutputDTO();
				contextMapping.context = transformer.convert(readProcessContext(mapping.context.processContextName));
				contextMapping.joinConditions = mapping.joinConditions;
				contextMapping.primaryStream = mapping.primaryStream;
				contextMapping.nestedField = mapping.nestedField;
				contextMapping.computedStates = (ArrayList<ComputedStateDTO>) mapping.computedStates;
				contextMapping.revisedFields = (ArrayList<RevisedFieldDTO>) mapping.revisedFields;
				contextMapping.persistedFields = (ArrayList<PersistedFieldDTO>) mapping.persistedFields;
				contextMapping.externalLookUp = mapping.externalLookUp;
				contextMapping.externalLookUp.urlTag = (ArrayList<URLTagDTO>) mapping.externalLookUp.urlTag;
				outputElem.add(contextMapping);
			}
			streamConfig.contextMappings = (ArrayList<ContextMappingOutputDTO>) outputElem;
			output.add(streamConfig);
		}
		globalConfig.globalConfig = (ArrayList<StreamConfigurationDTO>) output;

		return logger.traceExit(globalConfig);
	}

	/**
	 * Database Operation to Create Configuration Details
	 * 
	 * @param dto ConfigurationDetailsDTO
	 * @return ConfigurationDetails
	 * @throws SQLException SQLException
	 */
	public ConfigurationDetails create(ConfigurationDetailsDTO dto) throws SQLException {

		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();
		ConfigurationDetails daoObj = transformer.convert(dto);

		Dao<ConfigurationDetails, String> dao = DaoManager.createDao(DBConnection.getConnection(),
				ConfigurationDetails.class);
		dao.assignEmptyForeignCollection(daoObj, "configurationProperties");
		for (ConfigurationPropertiesDTO idDto : dto.configurationProperties) {
			daoObj.configurationProperties.add(transformer.convert(idDto));
		}

		daoObj = dao.createIfNotExists(daoObj);
		dao.refresh(daoObj);
		return logger.traceExit(daoObj);
	}

	/**
	 * Database Operation to Update Configuration Details
	 * 
	 * @param dtoObj ConfigurationDetailsDTO
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public Integer update(ConfigurationDetailsDTO dtoObj) throws SQLException {

		logger.traceEntry();
		ObjectConverter transformer = new ObjectConverter();
		ConfigurationDetails daoObj = transformer.convert(dtoObj);
		Integer output = 0;

		Dao<ConfigurationDetails, String> dao = DaoManager.createDao(DBConnection.getConnection(),
				ConfigurationDetails.class);
		dao.assignEmptyForeignCollection(daoObj, "configurationProperties");

		ConfigurationDetails perObj = dao.queryForId(dtoObj.configurationName);
		perObj.configurationProperties.clear();

		for (ConfigurationPropertiesDTO idDto : dtoObj.configurationProperties) {
			ConfigurationProperties configurationProperties = transformer.convert(idDto);
			configurationProperties.configurationDetails = perObj;
			daoObj.configurationProperties.add(configurationProperties);
		}
		output = dao.update(daoObj);

		return logger.traceExit(output);
	}

	/**
	 * Database Operation to Read Configuration Details
	 * 
	 * @param connName Connection Name 
	 * @return ConfigurationDetails
	 * @throws SQLException SQLException
	 */
	public ConfigurationDetails readConfigDetails(String connName) throws SQLException {

		logger.traceEntry();
		ConfigurationDetails eventObj = new ConfigurationDetails();
		Dao<ConfigurationDetails, String> dao = DaoManager.createDao(DBConnection.getConnection(),
				ConfigurationDetails.class);
		eventObj = dao.queryForId(connName);

		return logger.traceExit(eventObj);
	}

	/**
	 * Database Operation to Delete Configuration Details
	 * 
	 * @param connName Connection Name 
	 * @return 1(true)/0(false)
	 * @throws SQLException SQLException
	 */
	public Integer deleteConfDetails(String connName) throws SQLException {

		logger.traceEntry();
		Integer status = 0;

		Dao<ConfigurationDetails, String> dao = DaoManager.createDao(DBConnection.getConnection(),
				ConfigurationDetails.class);
		ConfigurationDetails eventObj = readConfigDetails(connName);
		if (null == eventObj) {
			throw new SQLException("Conf Details does not exist");
		}

		for (ConfigurationProperties configurationProperties : eventObj.configurationProperties) {
			delete(configurationProperties);
		}
		status = dao.deleteById(connName);
		return logger.traceExit(status);
	}
}