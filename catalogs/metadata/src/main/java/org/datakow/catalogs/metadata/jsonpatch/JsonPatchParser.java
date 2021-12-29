package org.datakow.catalogs.metadata.jsonpatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.springframework.util.StringUtils;

/**
 *
 * @author kevin.off
 */
public class JsonPatchParser {
    
    public static boolean applyPatch(List<JsonPatchOperation> operatioins, DotNotationMap target){

        for(JsonPatchOperation operation : operatioins){
            
            String jsonFrom = StringUtils.hasText(operation.getFrom()) ? operation.getFrom() : null;
            String from = jsonFrom != null ? jsonFrom.replaceFirst("/", "").replaceAll("/", ".") : null;
            String[] fromParts =  from != null ? from.split("\\.") : null;
            String subFrom = fromParts != null && fromParts.length > 0 
                    ? String.join(".", Arrays.asList(fromParts).subList(0, fromParts.length - 1)) 
                    : null;
            String fromPropertyName = fromParts != null && fromParts.length > 0 ? fromParts[fromParts.length - 1] : null;
            
            String jsonPath = StringUtils.hasText(operation.getPath()) ? operation.getPath() : null;
            String path = jsonPath != null ? jsonPath.replaceFirst("/", "").replaceAll("/", ".") : null;
            String[] pathParts =  path != null ? path.split("\\.") : null;
            String subPath = pathParts != null && pathParts.length > 0 
                    ? String.join(".", Arrays.asList(pathParts).subList(0, pathParts.length - 1)) 
                    : null;
            String pathPropertyName = pathParts != null && pathParts.length > 0 ? pathParts[pathParts.length - 1] : null;
            
            Object value = operation.getValue();
            if (value != null && value instanceof String){
                String val = (String)value;
                if (val.trim().startsWith("{")){
                    try {
                        value = DotNotationMap.fromJson(val);
                    } catch (JsonProcessingException ex) {
                        throw new JsonPatchException("The JSON object supplied in value could not be parsed", ex);
                    }
                }else if(val.trim().startsWith("[")){
                    try {
                        value = DotNotationList.fromJson(val);
                    } catch (JsonProcessingException ex) {
                        throw new JsonPatchException("The JSON object supplied in value could not be parsed", ex);
                    }
                }
            }
            
            switch(operation.getOperation()){
                case add:
                    add(target, subPath, pathPropertyName, path, value);
                    break;
                case remove:
                    remove(target, subPath, pathPropertyName, path);
                    break;
                case replace:
                    replace(target, subPath, pathPropertyName, path, value);
                    break;
                case copy:
                    copy(target, fromPropertyName, subFrom, from, pathPropertyName, path, subPath);
                    break;
                case move:
                    move(target, fromPropertyName, subFrom, from, pathPropertyName, path, subPath);
                    break;
                case test:
                    if (!test(target, path, value)){
                        return false;
                    }
                    break;
            }
        }
        return true;
    }
    
    protected static void add(DotNotationMap target, String subPath, String pathPropertyName, String path, Object value){
        
        if (path == null){
            throw new JsonPatchException("You must provide a value for path for an add operation");
        }
        if (value == null){
            throw new JsonPatchException("You must provide a value for an add operation");
        }
        
        if (!subPath.isEmpty() && !target.containsKey(subPath)){
            throw new JsonPatchException("The path " + subPath + " does not exist to perform an add on.");
        }
        
        // The sub path exists
        if (pathPropertyName.matches("^\\d+$") || pathPropertyName.equals("-")){
            //The property is a number or indicates a push to an array
            if (target.getProperty(subPath) instanceof List){
                if (pathPropertyName.equals("-")){
                    //indicates putting element at the end of the array
                    ((List)target.getProperty(subPath)).add(value);
                }else{
                    int index = Integer.valueOf(pathPropertyName);
                    if (index <= ((List)target.getProperty(subPath)).size()){
                        LinkedList theList = new LinkedList((List)target.getProperty(subPath));
                        theList.add(index, value);
                        target.setProperty(subPath, theList);
                    }else{
                        throw new JsonPatchException("You cannot specify an index greater than the size of the array for an add.");
                    }
                }
            }else{
                throw new JsonPatchException("You cannot add an array element to an object member.");
            }
        }else{
            target.setProperty(path, value);
        }
        
    }
    
    protected static void remove(DotNotationMap target, String subPath, String pathPropertyName, String path){
        
        if (path == null){
            throw new JsonPatchException("You must provide a value for path for a remove operation");
        }
        
        if (pathPropertyName.equals("-")){
            pathPropertyName = String.valueOf(((List)target.getProperty(subPath)).size() - 1);
            path = subPath + "." + pathPropertyName;
        }
        
        if (!target.containsKey(path)){
            throw new JsonPatchException("The path " + path + " does not exist to perform a remove on.");
        }
        
        if (pathPropertyName.matches("^\\d+$")){
            //The property is a number or indicates a push to an array
            if (target.getProperty(subPath) instanceof List){
                ((List)target.getProperty(subPath)).remove((int)Integer.valueOf(pathPropertyName));
            }else{
                throw new JsonPatchException("You cannot remove a member from an object by index.");
            }
        }else{
            target.remove(path);
        }
    }
    
    protected static void replace(DotNotationMap target, String subPath, String pathPropertyName, String path, Object value){
        
        if (path == null){
            throw new JsonPatchException("You must provide a value for path for a replace operation");
        }
        if (value == null){
            throw new JsonPatchException("You must provide a value for a replace operation");
        }
        
        if (pathPropertyName.equals("-")){
            pathPropertyName = String.valueOf(((List)target.getProperty(subPath)).size() - 1);
            path = subPath + "." + pathPropertyName;
        }
        
        if (!target.containsKey(path)){
            throw new JsonPatchException("The path " + path + " does not exist to perform a replace on.");
        }
        
        if (pathPropertyName.matches("^\\d+$")){
            //The property is a number or indicates a push to an array
            if (target.getProperty(subPath) instanceof List){
                target.setProperty(path, value);
            }else{
                throw new JsonPatchException("You cannot access a member from an object by index.");
            }
            
        }else{
            target.setProperty(path, value);
        }
    }
    
    protected static void copy(DotNotationMap target, String fromPropertyName, String subFrom, String from, String pathPropertyName, String path, String subPath){
        
        if (from == null){
            throw new JsonPatchException("You must provide a value for from for a copy operation");
        }
        if (path == null){
            throw new JsonPatchException("You must provide a value for path for a copy operation");
        }
        
        Object value;
        
        if (fromPropertyName.equals("-")){
            fromPropertyName = String.valueOf(((List)target.getProperty(subFrom)).size() - 1);
            from = subFrom + "." + fromPropertyName;
        }
        
        if (target.containsKey(from)){
            value = target.getProperty(from);
        }else{
            throw new JsonPatchException("The target does not contain the property " + from);
        }
        
        if (!subPath.isEmpty() && !target.containsKey(subPath)){
            throw new JsonPatchException("The path " + subPath + " does not exist to perform an add on.");
        }
        
        // The sub path exists
        if (pathPropertyName.matches("^\\d+$") || pathPropertyName.equals("-")){
            //The property is a number or indicates a push to an array
            if (target.getProperty(subPath) instanceof List){
                if (pathPropertyName.equals("-")){
                    //indicates putting element at the end of the array
                    ((List)target.getProperty(subPath)).add(value);
                }else{
                    int index = Integer.valueOf(pathPropertyName);
                    if (index <= ((List)target.getProperty(subPath)).size()){
                        List theList = new LinkedList((List)target.getProperty(subPath));
                        theList.add(index, value);
                        target.setProperty(subPath, theList);
                    }else{
                        throw new JsonPatchException("You cannot specify an index greater than the size of the array for an add.");
                    }
                }
            }else{
                throw new JsonPatchException("You cannot add an array element to an object member.");
            }
        }else{
            target.setProperty(path, value);
        }
        
    }
    
    protected static void move(DotNotationMap target, String fromPropertyName, String subFrom, String from, String pathPropertyName, String path, String subPath){

        if (from == null){
            throw new JsonPatchException("You must provide a value for from for a move operation");
        }
        if (path == null){
            throw new JsonPatchException("You must provide a value for path for a move operation");
        }
        
        Object value;
        
        if (fromPropertyName.equals("-")){
            fromPropertyName = String.valueOf(((List)target.getProperty(subFrom)).size() - 1);
            from = subFrom + "." + fromPropertyName;
        }
        
        if (target.containsKey(from)){
            value = target.getProperty(from);
        }else{
            throw new JsonPatchException("The target does not contain the property " + from);
        }
        
        if (!subPath.isEmpty() && !target.containsKey(subPath)){
            throw new JsonPatchException("The path " + subPath + " does not exist to perform a move to.");
        }
        
        if (!subFrom.isEmpty() && !target.containsKey(subFrom)){
            throw new JsonPatchException("The path " + subFrom + " does not exist to perform a move from.");
        }
        
        if (fromPropertyName.matches("^\\d+$")){
            //The property is a number or indicates a push to an array
            
            if (target.getProperty(subFrom) instanceof List){
                ((List)target.getProperty(subFrom)).remove((int)Integer.valueOf(fromPropertyName));
            }else{
                throw new JsonPatchException("You cannot remove a member from an object by index.");
            }
        }else{
            target.remove(from);
        }
        
        // The sub path exists
        if (pathPropertyName.matches("^\\d+$") || pathPropertyName.equals("-")){
            //The property is a number or indicates a push to an array
            if (target.getProperty(subPath) instanceof List){
                if (pathPropertyName.equals("-")){
                    //indicates putting element at the end of the array
                    ((List)target.getProperty(subPath)).add(value);
                }else{
                    int index = Integer.valueOf(pathPropertyName);
                    if (index <= ((List)target.getProperty(subPath)).size()){
                        List theList = new LinkedList((List)target.getProperty(subPath));
                        theList.add(index, value);
                        target.setProperty(subPath, theList);
                    }else{
                        throw new JsonPatchException("You cannot specify an index greater than the size of the array for an add.");
                    }
                }
            }else{
                throw new JsonPatchException("You cannot add an array element to an object member.");
            }
        }else{
            target.setProperty(path, value);
        }
        
    }
    
    protected static boolean test(DotNotationMap target, String path, Object value){
        
        if (path == null){
            throw new JsonPatchException("You must provide a value for path for a test operation");
        }
        
        if (value == null){
            return target.containsKey(path) && target.getProperty(path) == null;
        }else{
            return value.equals(target.getProperty(path));
        }
    }
    
}
