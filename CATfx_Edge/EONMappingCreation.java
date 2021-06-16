/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.eon;

import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.beans.mm.Mapping;
import com.icc.util.RequestStatus;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author AmanSingh
 */
public class EONMappingCreation {
    public static final String TRANSFORMATION_JOBS_KEY="transformationJobs";
    public static final String CONNECTORS_KEY="connectors";
    public static final String COMPONENTS_KEY="components";
    public static final String PARAMETER_KEY="parameters";
    public static final String ELEMENT_KEY="elements";
    public static final String NAME_KEY="name";
    public static final String ID_KEY="id";
    public static final String VALUE_KEY="value";
    public static final String VALUES_KEY="values";
    public static final String SOURCE_ID_KEY="sourceID";
    public static final String TARGET_ID_KEY="targetID";
    public static final String NAME_VALUE="Name";
    public static final String TARGET_TABLE_VALUE="Target Table"; 
    public static final String COLUMN_NAME_VALUE="Column Names";
    public static final String SCHEMA_VALUE="Schema";
    public static final String DATABASE_VALUE="Database";
    public static final String OFFSET_VALUE="Offset";
    public static final String METHOD_VALUE="Method";
    public static final String CAST_TYPES_VALUE="Cast Types";
    public static final String ADD_SOURCE_COMPONENT_COLUMN_VALUE="Add Source Component Column";
    public static final String REMOVE_DUPLICATES_VALUE="Remove duplicates";
    public static final String GROUPINGS_VALUE="Groupings";
    public static final String AGGREGATIONS_VALUE="Aggregations";
    public static final String PATTERN_TYPE_VALUE="Pattern Type";
    public static final String PATTERN_VALUE="Pattern";
    public static final String COLUMN_MAPPING_VALUE="Column Mapping";
    public static final String TRUNCATE_VALUE="Truncate";
    public static final String ADD_SOURCE_TABLE_VALUE="Add Source Table";
    public static final String COLUMNS_VALUE="Columns";
    public static final String INCLUDE_INPUT_COLUMNS_VALUE="Include Input Columns";
    public static final String CALCULATIONS_VALUE="Calculations";
    public static final String FILTER_CONDITIONS_VALUE="Filter Conditions";
    public static final String COMBINE_CONDITIONS_VALUE="Combine Conditions";
    public static final String WAREHOUSE_VALUE="Warehouse";
    public static final String FIX_DATA_TYPE_MISMACHES="Fix Data Type Mismatches";
    
     public  String eonMappingCreation(String filePath,MappingManagerUtil mmutil, String projectId){
        List<String> result=new ArrayList<>();
        try {
             
             String data  = new String(Files.readAllBytes(Paths.get(filePath))); 
             JSONObject sourceJSON = new JSONObject(data);
             
             JSONArray transformationJobs=sourceJSON.getJSONArray(TRANSFORMATION_JOBS_KEY);
             Map<String, List<String>> connectorMapping=getConnectorMapping(transformationJobs,result);
             Map<String, Map<String, String>> componentMapping=getComponentMapping(transformationJobs,result);
            
             connectorMapping.keySet().forEach(key->{
                 List<String> connectorList=connectorMapping.get(key);
                 ArrayList<MappingSpecificationRow> rowlist = new ArrayList<>();
                 connectorList.stream().forEach(connector->{
                       String[] ids=connector.split("#");
                       String sourceId=ids[0];
                       String targetId=ids[1];
                       Map<String, String> sourceInfo=componentMapping.get(sourceId);
                       Map<String, String> targetInfo=componentMapping.get(targetId);
                      
                       String columns =(String) sourceInfo.get(COLUMNS_VALUE);
                       String[] columnsarr=null;
                       if(columns==null || columns.isEmpty()){
                           columnsarr=new String[1];
                           columnsarr[0]="dummy";
                       }else{
                             columnsarr= columns.split("#");
                       }
                      
                       
                       for (String columnName : columnsarr) {
                            MappingSpecificationRow row = new MappingSpecificationRow();
                            String sourceTableName=sourceInfo.get(NAME_VALUE);
                            String targetTableName=targetInfo.get(NAME_VALUE);
                            row.setSourceTableName(sourceTableName);
                            row.setTargetTableName(targetTableName);
                            row.setSourceColumnName(columnName);
                            row.setTargetColumnName(columnName);
                            row.setSourceSystemName(sourceInfo.get(DATABASE_VALUE)==null?"SYSTEM":sourceInfo.get(DATABASE_VALUE));
                            row.setSourceSystemEnvironmentName(sourceInfo.get(SCHEMA_VALUE)==null?"ENVIRONMENT":sourceInfo.get(SCHEMA_VALUE));
                            row.setTargetSystemName(targetInfo.get(DATABASE_VALUE)==null?"SYSTEM":targetInfo.get(DATABASE_VALUE));
                            row.setTargetSystemEnvironmentName(targetInfo.get(SCHEMA_VALUE)==null?"ENVIRONMENT":targetInfo.get(SCHEMA_VALUE));
                            rowlist.add(row);
                       }
                       
                       
                       
                 });
                 result.add(createMappings(mmutil,rowlist,projectId,key,result));
                 
             });
             
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        return String.valueOf(result);
    }
    
     private String createMappings(MappingManagerUtil mmutil, ArrayList<MappingSpecificationRow> mappingSpecifications, String projectId, String mapName,List<String> result){
         RequestStatus rs = null;
        int subId = 0;
        int subSubId = 0;
        try {
            Mapping mapping = new Mapping();
            mapping.setProjectId(Integer.parseInt(projectId));
            mapping.setMappingName(mapName);
            
            
            mapping.setMappingSpecifications(mappingSpecifications);
            rs = mmutil.createMapping(mapping);
        } catch (Exception e) {
            result.add(e.getMessage());
            e.printStackTrace();
        }
        return rs.getStatusMessage();
     }
      private Map<String, List<String>> getConnectorMapping(JSONArray transformationJobs,List<String> result){
          Map<String, List<String>> connectorMapping=new ConcurrentHashMap<>();
          try {
             for(int i=0;i<transformationJobs.length();i++){
                 JSONObject transformationJob=transformationJobs.getJSONObject(i);
                 JSONObject connectors=transformationJob.getJSONObject(CONNECTORS_KEY);
                 String transformationJobId=String.valueOf(transformationJob.getInt(ID_KEY));
                 List<String> connectorList=new ArrayList<>();
                 connectors.keys().forEachRemaining(key->{
                     try {
                            JSONObject connector=connectors.getJSONObject((String) key);
                            connectorList.add(connector.getInt(SOURCE_ID_KEY)+"#"+connector.getInt(TARGET_ID_KEY));
                     } catch (Exception e) {
                         result.add(e.getMessage());
                         e.printStackTrace();
                     }
                     
                 });
                 
                 connectorMapping.put(transformationJobId, connectorList);
          }
         } catch (Exception e) {
             result.add(e.getMessage());
             e.printStackTrace();
         }
          
          return connectorMapping;
     }
    
     private Map<String, Map<String, String>> getComponentMapping(JSONArray transformationJobs,List<String> result){
         Map<String, Map<String, String>> componentMapping=new ConcurrentHashMap<>();
          try {
             for(int i=0;i<transformationJobs.length();i++){
                 JSONObject transformationJob=transformationJobs.getJSONObject(i);
                 JSONObject components=transformationJob.getJSONObject(COMPONENTS_KEY);
                 components.keys().forEachRemaining(key->{
                     try {
                            Map<String, String> componentMap=getDefaultComponentMap();
                            JSONObject component=components.getJSONObject((String) key);
                            JSONObject parameters=component.getJSONObject(PARAMETER_KEY);
                            parameters.keys().forEachRemaining(keys->{
                                try {
                                        JSONObject parameter=parameters.getJSONObject((String) keys);
                                        String elementType=parameter.getString(NAME_KEY);
                                        JSONObject elements=parameter.getJSONObject(ELEMENT_KEY);
                                        setElementValues(elementType, elements, componentMap,result);
                                } catch (Exception e) {
                                    result.add(e.getMessage());
                                    e.printStackTrace();
                                }
                               
                            });
                        componentMapping.put((String) key,componentMap);
                     } catch (Exception e) {
                         result.add(e.getMessage());
                         e.printStackTrace();
                     }
                     
                     
                 });
                 
          }
         } catch (Exception e) {
             result.add(e.getMessage());
             e.printStackTrace();
         }
          
          return componentMapping;
     }
     
    private void setElementValues(String elementType,JSONObject elements,Map<String, String> componentMapping,List<String> result) {
        StringBuilder componentNames=new StringBuilder();
        boolean[] isFirst={true};
        elements.keys().forEachRemaining(key->{
            try {
                JSONObject element=elements.getJSONObject((String) key);
            JSONObject values=element.getJSONObject(VALUES_KEY);
            JSONObject value=values.getJSONObject("1");
            if(isFirst[0]){
                isFirst[0]=false;
            }else{
                componentNames.append("#");
            }
            componentNames.append(value.getString(VALUE_KEY));
            } catch (Exception e) {
                result.add(e.getMessage());
                e.printStackTrace();
                
            }
            
        });
        componentMapping.put(elementType, componentNames.toString());
    }
    private Map<String, String> getDefaultComponentMap(){
        Map<String, String> defaultComponent=new HashMap<>();
        defaultComponent.put(NAME_VALUE, null);
        defaultComponent.put(TARGET_TABLE_VALUE, null);
        defaultComponent.put(COLUMN_NAME_VALUE, null);
        defaultComponent.put(SCHEMA_VALUE, null);
        defaultComponent.put(DATABASE_VALUE, null);
        defaultComponent.put(OFFSET_VALUE, null);
        defaultComponent.put(METHOD_VALUE, null);
        defaultComponent.put(CAST_TYPES_VALUE, null);
        defaultComponent.put(ADD_SOURCE_COMPONENT_COLUMN_VALUE, null);
        defaultComponent.put(REMOVE_DUPLICATES_VALUE, null);
        defaultComponent.put(GROUPINGS_VALUE, null);
        defaultComponent.put(AGGREGATIONS_VALUE, null);
        defaultComponent.put(PATTERN_TYPE_VALUE, null);
        defaultComponent.put(PATTERN_VALUE, null);
        defaultComponent.put(COLUMN_MAPPING_VALUE, null);
        defaultComponent.put(PATTERN_VALUE, null);
        defaultComponent.put(TRUNCATE_VALUE, null);
        defaultComponent.put(ADD_SOURCE_TABLE_VALUE, null);
        defaultComponent.put(COLUMNS_VALUE, null);
        defaultComponent.put(INCLUDE_INPUT_COLUMNS_VALUE, null);
        defaultComponent.put(CALCULATIONS_VALUE, null);
        defaultComponent.put(FILTER_CONDITIONS_VALUE, null);
        defaultComponent.put(COMBINE_CONDITIONS_VALUE, null);
        defaultComponent.put(WAREHOUSE_VALUE, null);
        defaultComponent.put(FIX_DATA_TYPE_MISMACHES, null);
        return defaultComponent;
                
    }
}
