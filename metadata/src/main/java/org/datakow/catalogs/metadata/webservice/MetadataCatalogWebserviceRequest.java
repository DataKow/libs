package org.datakow.catalogs.metadata.webservice;

import org.datakow.catalogs.metadata.database.MetadataDataCoherence;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriUtils;

/**
 *
 * @author kevin.off
 */
public class MetadataCatalogWebserviceRequest {

    Map<String, Object> rawParams = new LinkedHashMap<>();

    public static MetadataCatalogWebserviceRequest builder(){
        return new MetadataCatalogWebserviceRequest();
    }
    
    public MetadataCatalogWebserviceRequest withQuery(String fiql){
        if (StringUtils.hasText(fiql)){
            this.rawParams.put("s", fiql);
        }
        return this;
    }
    
    public MetadataCatalogWebserviceRequest withSort(List<String> sortString){
        if (sortString != null && !sortString.isEmpty()){
            this.rawParams.put("sort", sortString);
        }
        return this;
    }
    
    public MetadataCatalogWebserviceRequest withSort(String sortStringList){
        if (StringUtils.hasText(sortStringList)){
            this.rawParams.put("sort", sortStringList);
        }
        return this;
    }

    public MetadataCatalogWebserviceRequest withLimit(Integer limit) {
        if (limit != null && limit >= 0){
            this.rawParams.put("limit", limit);
        }
        return this;
    }

    @Deprecated
    public MetadataCatalogWebserviceRequest withGroupBy(String groupBy) {
        if (StringUtils.hasText(groupBy)){
            this.rawParams.put("groupBy", groupBy);
        }
        return this;
    }

    @Deprecated
    public MetadataCatalogWebserviceRequest withGroupFunctions(List<String> groupFunctionList){
        if (groupFunctionList != null && !groupFunctionList.isEmpty()){
            this.rawParams.put("groupFunc", groupFunctionList);
        }
        return this;
    }
    
    @Deprecated
    public MetadataCatalogWebserviceRequest withGroupFunctions(String groupFunctionsString){
        if (StringUtils.hasText(groupFunctionsString)){
            this.rawParams.put("groupFunc", groupFunctionsString);
        }
        return this;
    }

    @Deprecated
    public MetadataCatalogWebserviceRequest withGroupSort(List<String> groupSortList){
        if (groupSortList != null && !groupSortList.isEmpty()){
            this.rawParams.put("groupSort", groupSortList);
        }
        return this;
    }
    
    @Deprecated
    public MetadataCatalogWebserviceRequest withGroupSort(String groupSortString){
        if (StringUtils.hasText(groupSortString)){
            this.rawParams.put("groupSort", groupSortString);
        }
        return this;
    }

    @Deprecated
    public MetadataCatalogWebserviceRequest withNear(String near){
        if (StringUtils.hasText(near)){
            this.rawParams.put("geoNear", near);
        }
        return this;
    }
    
    @Deprecated
    public MetadataCatalogWebserviceRequest withNear(Double lon, Double lat, Integer geoNearMaxDistance) {
        if (lon != null && lat != null && geoNearMaxDistance != null){
            this.rawParams.put("geoNear", "(" + String.valueOf(lon) + "," + String.valueOf(lat) + "," + String.valueOf(geoNearMaxDistance) + ")");
        }
        return this;
        
    }

    public MetadataCatalogWebserviceRequest withProjectionProperties(List<String> projectionProperties){
        if (projectionProperties != null && !projectionProperties.isEmpty()){
            this.rawParams.put("properties", projectionProperties);
        }
        return this;
    }
    
    public MetadataCatalogWebserviceRequest withProjectionProperties(String properties){
        if (StringUtils.hasText(properties)){
            this.rawParams.put("properties", properties);
        }
        return this;
    }

    public MetadataCatalogWebserviceRequest withDataCoherence(MetadataDataCoherence coherence) {
        if (coherence != null){
            this.rawParams.put("dataCoherence", coherence);
        }
        return this;
    }
    
    public MetadataCatalogWebserviceRequest withDistinct(String propertyName){
        if (StringUtils.hasText(propertyName)){
            this.rawParams.put("distinct", propertyName);
        }
        return this;
    }
    
    public MetadataCatalogWebserviceRequest withMulti(boolean multi){
        this.rawParams.put("multi", multi ? "true" : "false");
        return this;
    }
    
    public MetadataCatalogWebserviceRequest withUpsert(boolean upsert){
        this.rawParams.put("upsert", upsert ? "true" : "false");
        return this;
    }
    
    public MetadataCatalogWebserviceRequest withPipeline(String pipeline){
        if (StringUtils.hasText(pipeline)){
            this.rawParams.put("pipeline", pipeline);
        }
        return this;
    }
    
    public MetadataCatalogWebserviceRequest withFilter(String filter){
        if (StringUtils.hasText(filter)){
            this.rawParams.put("filter", filter);
        }
        return this;
    }
    
    public MetadataCatalogWebserviceRequest withProperty(String propertyName, Object propertyValue){
                 //not null       and                 not a string                            or              is not empty
        if (propertyValue != null && (!String.class.isAssignableFrom(propertyValue.getClass()) || StringUtils.hasText((String)propertyValue))){
            // Dont put an empty list in there
            if (propertyValue instanceof List){
                if (((List)propertyValue).isEmpty()){
                    return this;
                }
            }
            this.rawParams.put(propertyName, propertyValue);
        }
        return this;
    }

    public String getQueryParamsString(boolean encode){
    
        if (encode){
            return getQueryParams().entrySet().stream().map((kv) -> { 
                try{
                    return kv.getKey() + "=" + UriUtils.encodeQueryParam(kv.getValue(), "UTF-8");
                }catch(IllegalArgumentException ex){
                    Logger.getLogger(MetadataCatalogWebserviceRequest.class.getName()).log(
                        Level.SEVERE, 
                        "Could not encode query string parameter: " + kv.getValue(), 
                        ex);
                    throw new IllegalArgumentException("Could not encode query string parameter: " + kv.getValue());
                }
            }).collect(Collectors.joining("&"));
        }else{
            return getQueryParams().entrySet().stream().map((kv) -> kv.getKey() + "=" + kv.getValue()).collect(Collectors.joining("&"));
        }
    }
    
    public Map<String, String> getQueryParams(){
        Map<String, String> params = new LinkedHashMap<>();
        
        this.rawParams.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    String value;
                    if (entry.getValue() instanceof List){
                        value = String.join(",", (List<String>)entry.getValue());
                    }else{
                        value = String.valueOf(entry.getValue());
                    }
                    params.put(entry.getKey(), value);
                });

        return params;
    }
    
    public String toUrl(String baseUrl){
        String queryString = getQueryParamsString(false);
        return baseUrl + (!StringUtils.hasText(queryString) ? "" : "?" + queryString);
    }
    
    public URI toUri(String baseUrl){
        try {
            String queryString = getQueryParamsString(true);
            URI uri = new URI(baseUrl + (!StringUtils.hasText(queryString) ? "" : "?" + queryString));
            return uri;
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("Could not parse url: " + baseUrl + "?" + getQueryParamsString(false), ex);
        }
    }
    
}
