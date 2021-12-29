package org.datakow.catalogs.metadata.database.converters;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.geo.GeoJsonLineString;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.query.GeoCommand;

/**
 * Spring MongoDB converter that converts a {@link GeoCommand} to a {@link DBObject}
 * when creating a query from a {@link org.springframework.data.mongodb.core.query.Criteria} object.
 * <p>
 * There was no converter for the {@link GeoCommand} class. This converter
 * also takes care of formatting.
 * <p>
 * Only supports polygons!!!!
 * 
 * @author kevin.off
 */
public class GeoCommandToDbObjectConverter implements Converter<GeoCommand, Document>{

    /**
     * Spring MongoDB converter that converts a {@link GeoCommand} to a {@link DBObject}
     * when creating a query from a {@link org.springframework.data.mongodb.core.query.Criteria} object.
     * <p>
     * There was no converter for the {@link GeoCommand} class. This converter
     * also takes care of formatting.
     * <p>
     * Only supports polygons and points!!!!
     *  
     * @param command The GeoCommand to convert
     * @return The converted GeoCommand
     */
    @Override
    public Document convert(GeoCommand command) {
        Document returnObject = new Document();
        switch(command.getCommand()){
            case "$polygon":
                GeoJsonPolygon polygon = (GeoJsonPolygon)command.getShape();
                
                List<Double[][]> polygonCoords = new ArrayList<>();

                int i;
                for(GeoJsonLineString polygonPart : polygon.getCoordinates()){
                    i = 0;
                    Double[][] points = new Double[polygon.getCoordinates().get(0).getCoordinates().size()][2];
                    for(Point point : polygonPart.getCoordinates()){
                        points[i][0] = point.getX();
                        points[i][1] = point.getY();
                        i++;
                    }
                    polygonCoords.add(points);
                }
                returnObject.put("type", "Polygon");
                returnObject.put("coordinates", polygonCoords);
                break;
            case "$point":
                GeoJsonPoint point = (GeoJsonPoint)command.getShape();
                Double[] pointCoords = new Double[2];
                point.getCoordinates().toArray(pointCoords);
                returnObject.put("type", "Polygon");
                returnObject.put("coordinates", pointCoords);
                break;
        }
        
        return returnObject;
    }
    
}
