/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.eon;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

/**
 *
 * @author AmanSingh
 */
public class ComponentParseThread implements  Runnable{
    private Map<String, Map<String, String>> componentMapping;
    private JSONObject transformationJob;
    private List<String> result;
    @Override
    public void run() {
        getComponentMapping();
    }
  
    
    private void getComponentMapping(){
        try {
            JSONObject components = transformationJob.getJSONObject(EONMappingCreation.COMPONENTS_KEY);
            components.keys().forEachRemaining(key -> {
                try {
                    Map<String, String> componentMap = getDefaultComponentMap();
                    JSONObject component = components.getJSONObject((String) key);
                    JSONObject parameters = component.getJSONObject(EONMappingCreation.PARAMETER_KEY);
                    parameters.keys().forEachRemaining(keys -> {
                        try {
                            JSONObject parameter = parameters.getJSONObject((String) keys);
                            String elementType = parameter.getString(EONMappingCreation.NAME_KEY);
                            JSONObject elements = parameter.getJSONObject(EONMappingCreation.ELEMENT_KEY);
                            setElementValues(elementType, elements, componentMap);
                        } catch (Exception e) {
                            result.add(e.getMessage());
                            e.printStackTrace();
                        }
                    });
                    componentMapping.put((String) key, componentMap);
                } catch (Exception e) {
                    result.add(e.getMessage());
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            result.add(e.getMessage());
            e.printStackTrace();
        }
     }
    private void setElementValues(String elementType,JSONObject elements,Map<String, String> componentMapping) {
        StringBuilder componentNames=new StringBuilder();
        boolean[] isFirst={true};
        elements.keys().forEachRemaining(key->{
            try {
                JSONObject element=elements.getJSONObject((String) key);
            JSONObject values=element.getJSONObject(EONMappingCreation.VALUES_KEY);
            JSONObject value=values.getJSONObject("1");
            if(isFirst[0]){
                isFirst[0]=false;
            }else{
                componentNames.append("#");
            }
            componentNames.append(value.getString(EONMappingCreation.VALUE_KEY));
            } catch (Exception e) {
                result.add(e.getMessage());
                e.printStackTrace();
                
            }
            
        });
        componentMapping.put(elementType, componentNames.toString());
    }
    private Map<String, String> getDefaultComponentMap(){
        Map<String, String> defaultComponent=new HashMap<>();
        defaultComponent.put(EONMappingCreation.NAME_VALUE, null);
        defaultComponent.put(EONMappingCreation.TARGET_TABLE_VALUE, null);
        defaultComponent.put(EONMappingCreation.COLUMN_NAME_VALUE, null);
        defaultComponent.put(EONMappingCreation.SCHEMA_VALUE, null);
        defaultComponent.put(EONMappingCreation.DATABASE_VALUE, null);
        defaultComponent.put(EONMappingCreation.OFFSET_VALUE, null);
        defaultComponent.put(EONMappingCreation.METHOD_VALUE, null);
        defaultComponent.put(EONMappingCreation.CAST_TYPES_VALUE, null);
        defaultComponent.put(EONMappingCreation.ADD_SOURCE_COMPONENT_COLUMN_VALUE, null);
        defaultComponent.put(EONMappingCreation.REMOVE_DUPLICATES_VALUE, null);
        defaultComponent.put(EONMappingCreation.GROUPINGS_VALUE, null);
        defaultComponent.put(EONMappingCreation.AGGREGATIONS_VALUE, null);
        defaultComponent.put(EONMappingCreation.PATTERN_TYPE_VALUE, null);
        defaultComponent.put(EONMappingCreation.PATTERN_VALUE, null);
        defaultComponent.put(EONMappingCreation.COLUMN_MAPPING_VALUE, null);
        defaultComponent.put(EONMappingCreation.PATTERN_VALUE, null);
        defaultComponent.put(EONMappingCreation.TRUNCATE_VALUE, null);
        defaultComponent.put(EONMappingCreation.ADD_SOURCE_TABLE_VALUE, null);
        defaultComponent.put(EONMappingCreation.COLUMNS_VALUE, null);
        defaultComponent.put(EONMappingCreation.INCLUDE_INPUT_COLUMNS_VALUE, null);
        defaultComponent.put(EONMappingCreation.CALCULATIONS_VALUE, null);
        defaultComponent.put(EONMappingCreation.FILTER_CONDITIONS_VALUE, null);
        defaultComponent.put(EONMappingCreation.COMBINE_CONDITIONS_VALUE, null);
        defaultComponent.put(EONMappingCreation.WAREHOUSE_VALUE, null);
        defaultComponent.put(EONMappingCreation.FIX_DATA_TYPE_MISMACHES, null);
        return defaultComponent;        
    }
}
