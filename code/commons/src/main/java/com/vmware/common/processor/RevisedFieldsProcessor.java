package com.vmware.common.processor;

import javax.script.ScriptException;

import com.jayway.jsonpath.DocumentContext;
import com.vmware.common.dim.ScriptEngineCache;
import com.vmware.common.dto.ContextMappingOutputDTO;
import com.vmware.common.dto.RevisedFieldDTO;

public class RevisedFieldsProcessor {

	public static void process(DocumentContext streamDocumentContext, ContextMappingOutputDTO contextMappingOutputDTO) throws Exception{
		
		if(contextMappingOutputDTO.revisedFields != null && !contextMappingOutputDTO.revisedFields.isEmpty()){
			for(RevisedFieldDTO revisedFieldDTO : contextMappingOutputDTO.revisedFields){
				Object value = ScriptEngineCache.get().evalExpr(streamDocumentContext, revisedFieldDTO.expression);
				if(value != null){
					streamDocumentContext.set(revisedFieldDTO.fieldName, value.toString());
				}				
			}
		}
	}
}
