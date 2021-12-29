package org.datakow.fiql;

import cz.jirutka.rsql.parser.ast.ComparisonOperator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.geo.GeoJson;
import org.springframework.data.mongodb.core.geo.GeoJsonMultiPolygon;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.query.Criteria;


/**
 * Class responsible for building the Mongo {@link Criteria} object. 
 * The Criteria object is passed here over and over and the new operator
 * is appended to the criteria.
 * 
 * @author kevin.off
 */
public class MongoCriteriaOperatorAppender {
    
    private static final Map<ComparisonOperator, OperatorApplication> operators = new HashMap<>();
    
    /**
     * Creates the instance which creates a list of lambda functions, one for each
     * FiqlOperator.
     * <p>
     * When the apply function is called, the appropriate function is fetched
     * and executed. 
     */
    public MongoCriteriaOperatorAppender(){
        operators.put( FiqlOperator.EQUAL,                 (criteria, arg)-> { return criteria.is(arg);  });
        operators.put( FiqlOperator.GREATER_THAN_OR_EQUAL, (criteria, arg)-> { return criteria.gte(arg); });
        operators.put( FiqlOperator.GREATER_THAN,          (criteria, arg)-> { return criteria.gt(arg);  });
        operators.put( FiqlOperator.LESS_THAN_OR_EQUAL,    (criteria, arg)-> { return criteria.lte(arg); });
        operators.put( FiqlOperator.LESS_THAN,             (criteria, arg)-> { return criteria.lt(arg);  });
        operators.put( FiqlOperator.NOT_EQUAL,             (criteria, arg)-> { return criteria.ne(arg);  });
        operators.put( FiqlOperator.IN,                    (criteria, arg)-> { 
            Collection val;
            if (Collection.class.isAssignableFrom(arg.getClass())){
                val = (Collection)arg;
            }else{
                val = new ArrayList(Arrays.asList(arg));
            }
            return criteria.in(val);  
        });
        operators.put( FiqlOperator.NOT_IN,                (criteria, arg)-> { 
            Collection val;
            if (Collection.class.isAssignableFrom(arg.getClass())){
                val = (Collection)arg;
            }else{
                val = new ArrayList(Arrays.asList(arg));
            }
            return criteria.nin(val); 
        });
        operators.put( FiqlOperator.ALL,                   (criteria, arg)-> { 
            Collection val;
            if (Collection.class.isAssignableFrom(arg.getClass())){
                val = (Collection)arg;
            }else{
                val = new ArrayList(Arrays.asList(arg));
            }
            return criteria.all(val); 
        });
        operators.put( FiqlOperator.LIKE,                  (criteria, arg)-> { return criteria.regex(arg.toString()); });
        operators.put( FiqlOperator.MATCH,                 (criteria, arg)-> { 
            MongoFiqlParser p = new MongoFiqlParser();
            String fiql = (String)arg;
            return criteria.elemMatch(p.parse(fiql));
        });
        operators.put( FiqlOperator.WITHIN,                (criteria, arg)-> { 
            if (arg.getClass() != String.class){
                throw new IllegalArgumentException("Your polygon argument must be a string");
            }
            Polygon shape = parseWKTPolygon((String)arg);
            return criteria.within(shape);
        });
        operators.put( FiqlOperator.NEAR,                (criteria, arg)-> { 
            List points = (List)arg;
            if (points.size() != 3){
                throw new IllegalArgumentException("You must specify 3 arguments in a NEAR query");
            }
            if (!Number.class.isAssignableFrom(points.get(0).getClass()) || !Number.class.isAssignableFrom(points.get(1).getClass()) || !Number.class.isAssignableFrom(points.get(2).getClass())){
                throw new IllegalArgumentException("All three arguments to a near query must be numbers.");
            }
            GeoJsonPoint point1 = new GeoJsonPoint(((Number)points.get(0)).doubleValue(), ((Number)points.get(1)).doubleValue());
            Double maxDistance =  ((Number)points.get(2)).doubleValue();
            return criteria.nearSphere(point1).maxDistance(maxDistance);
        });
        operators.put( FiqlOperator.INTERSECT,          (criteria, arg)-> {
            GeoJson geo = parseWKT((String)arg);
            return criteria.intersects(geo);
        });
        operators.put( FiqlOperator.EXISTS,             (criteria, arg)-> {
            return criteria.exists((boolean)arg);
        });
        operators.put(FiqlOperator.TYPE, (criteria,arg) -> {
            int mongoType;
            if (arg instanceof String){
                mongoType = convertMongoType((String)arg);
            }else{
                mongoType = (int)arg;
            }
            return criteria.type(mongoType);
        });
    }
    
    /**
     * The method used on every node of the logical chain in order to add a
     * new Criteria object to the chain.
     * 
     * @param operator The operator that is being applied
     * @param criteria The criteria to add the new criteria objec to
     * @param argument The argument of the operation to apply
     * @return The resulting criteria
     */
    public Criteria apply(ComparisonOperator operator, Criteria criteria, Object argument){
        OperatorApplication app = operators.get(operator);
        return app.apply(criteria, argument);
    }
    
    /**
     * A Functional Interface that exposes an apply function to call.
     * This interface allows the list of operator functions to have a method to call.
     * 
     */
    @FunctionalInterface
    private interface OperatorApplication{
        Criteria apply(Criteria criteria, Object argument);
    }
    
    /**
     * Parses a Well Known Text string and converts it to a GeoJson object
     * to be used in a Mongo query.
     * 
     * @param wkt The Well Known Text string to pares
     * @return The parsed string
     */
    public GeoJson parseWKT(String wkt){
        String type = wkt.substring(0, wkt.indexOf("(")).trim().toUpperCase();
        String body = wkt.substring(wkt.indexOf("("));
        switch(type){
            case "POINT":
                return parseWKTPoint(body);
            case "POLYGON":
                return parseWKTPolygon(body);
            case "MULTIPOLYGON":
                return parseWKTMultiPolygon(body);
            default:
                break;
        }
        return null;
    }
    
    /**
     * Parses the Well Known Text of a Point and converts it to a GeoJsonPoint
     * to use in the Mongo Query.
     * 
     * @param body The text to parse
     * @return The parsed object
     */
    public GeoJsonPoint parseWKTPoint(String body){
        String[] xAndY = body.replaceFirst(" ?POINT ?", "").replace("(", "").replace(")", "").trim().split(" ");
        double x = Double.parseDouble(xAndY[0]);
        double y = Double.parseDouble(xAndY[1]);
        return new GeoJsonPoint(x, y);
    }
    
    /**
     * Parses the Well Known Text of a Polygon and converts it to a GeoJsonPolygon
     * to use in the Mongo Query.
     * 
     * @param body The text to parse
     * @return The parsed object
     */
    public GeoJsonPolygon parseWKTPolygon(String body){

        String pointsString = body.replaceFirst(" ?POLYGON ?", "").replace("((", "").replace("))", "");
        String[] pointStringArray = pointsString.split(",");
        List<Point> points = new ArrayList<>();
        for(String pointString : pointStringArray){
            points.add(parseWKTPoint(pointString));
        }
        return new GeoJsonPolygon(points);
    }
    
    /**
     * Parses the Well Known Text of a Multi Polygon and converts it to a GeoJsonMultiPolygon
     * to use in the Mongo Query.
     * 
     * @param wkt The text to parse
     * @return The parsed object
     */
    public GeoJsonMultiPolygon parseWKTMultiPolygon(String wkt){
        String polygonsString = wkt.trim().replaceFirst(" ?MULTIPOLYGON ?", "");
        polygonsString = polygonsString.substring(1, polygonsString.length() - 1);
        String[] polygonStringArray = polygonsString.split("\\)\\),");
        List<GeoJsonPolygon> polygons = new ArrayList<>();
        for (String polygonString : polygonStringArray){
            polygons.add(parseWKTPolygon(polygonString + "))"));
        }
        return new GeoJsonMultiPolygon(polygons);
    }
    
    int convertMongoType(String typeName){
        switch(typeName){
            case "double":
                return 1;
            case "string":
                return 2;
            case "object":
                return 3;
            case "array":
                return 4;
            case "binData":
                return 5;
            case "undefined":
                return 6;
            case "objectId":
                return 7;
            case "bool":
                return 8;
            case "date":
                return 9;
            case "null":
                return 10;
            case "regex":
                return 11;
            case "dbPointer":
                return 12;
            case "javascript":
                return 13;
            case "symbol":
                return 14;
            case "javascriptWithScope":
                return 15;
            case "int":
                return 16;
            case "timestamp":
                return 17;
            case "long":
                return 18;
            case "minKey":
                return -1;
            case "maxKey":
                return 127;
            default:
                throw new IllegalArgumentException("The mongotype " + typeName + " does not exist.");
        }
    }
    
}


