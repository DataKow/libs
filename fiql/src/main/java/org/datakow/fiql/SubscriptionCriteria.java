package org.datakow.fiql;


import org.datakow.core.components.DotNotationMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



/**
 * A criteria object that works in a similar way as the mongo {@link org.springframework.data.mongodb.core.query.Criteria}
 * object works.
 * You can apply logical AND, OR operators along with many comparison operators.
 * 
 * @author kevin.off
 */
public class SubscriptionCriteria {
    
    protected String propertyName;
    protected Object value;
    
    private final LinkedHashMap<String, Object> criteria = new LinkedHashMap<>();
    private final List<SubscriptionCriteria> chain;
    
    /**
     * Creates a new instance
     */
    public SubscriptionCriteria(){
        chain = new ArrayList<>();
    }
    
    /**
     * Creates a new instance starting with the name of the property to compare.
     * 
     * @param propertyName The name of the property
     */
    public SubscriptionCriteria(String propertyName){
        this.chain = new ArrayList<>();
        this.chain.add(this);
        this.propertyName = propertyName;
    }
    
    /**
     * Creates a new instance by adding this to the provided chain and setting
     * the property for the next comparison.
     * @param chain The chain to add to
     * @param propertyName The name of the property
     */
    protected SubscriptionCriteria(List<SubscriptionCriteria> chain, String propertyName){
        this.chain = chain;
        this.chain.add(this);
        this.propertyName = propertyName;
    }
    
    /**
     * Gets the property name in the comparison.
     * 
     * @return The name of the property
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * Sets the property name in the comparison
     * 
     * @param propertyName The name of the property
     */
    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    /**
     * Set the value to compare against
     * 
     * @param value The value to compare against
     */
    public void setValue(Object value) {
        this.value = value;
    }
    
    /**
     * Get the value to compare against
     * 
     * @param <T> The return type of the value
     * @return The value to compare against
     */
    public <T> T getValue(){
        return (T)this.value;
    }   
    
    /**
     * Compares the given Map against this subscription criteria to determine
     * if the map's properties meet the criteria.
     * 
     * @param notification The map to compare against
     * @return True if the Map meets the criteria
     */
    public boolean meetsCriteria(DotNotationMap notification) {
        if (this.chain.size() == 1 && this.propertyName == null){
            return this.chain.get(0).meetsCriteria(notification);
        }else if (propertyName.equalsIgnoreCase(LogicalOperator.AND)){
            for(SubscriptionCriteria crit : (List<SubscriptionCriteria>)this.getValue()){
                if (!crit.meetsCriteria(notification)){
                    //short circuit for false on AND
                    return false;
                }
            }
            return true;
        }else if (propertyName.equalsIgnoreCase(LogicalOperator.OR)){
            for(SubscriptionCriteria crit : (List<SubscriptionCriteria>)this.getValue()){
                if (crit.meetsCriteria(notification)){
                    //short circuit for true on OR
                    return true;
                }
            }
            return false;
        }else {
            boolean meets = true;
            
            
            Object docValue = notification.getProperty(propertyName);
            boolean docValueExists = notification.containsKey(propertyName);
            
            Class docValueClass = null;
            if (docValue != null){
                if (Collection.class.isAssignableFrom(docValue.getClass())){
                    if (!((Collection)docValue).isEmpty()){
                        docValueClass = ((Collection)docValue).iterator().next().getClass();
                    }
                }else{
                    docValueClass = docValue.getClass();
                }
            }
            
            Object convertedCriteriaValue;
            if (this.criteria.isEmpty()){
                //shows that this is a test for equality "=="
                convertedCriteriaValue = ObjectConverter.convert(this.value, docValueClass);
                if (docValue != null && Collection.class.isAssignableFrom(docValue.getClass())){
                    return ((Collection)docValue).contains(convertedCriteriaValue);
                }else{
                    return (docValueExists && docValue == null && this.value == null) || (convertedCriteriaValue != null && convertedCriteriaValue.equals(docValue));
                }
            }else{
                for(Entry<String, Object> crit : this.criteria.entrySet()){
                    switch (crit.getKey()){
                        //NOTE: Does not support java Date
                        case ComparisonOperator.GT:
                            convertedCriteriaValue = ObjectConverter.convert(crit.getValue(), docValueClass);
                            meets = docValue != null && ((Comparable)docValue).compareTo(convertedCriteriaValue) > 0;
                            break;
                        case ComparisonOperator.LT:
                            convertedCriteriaValue = ObjectConverter.convert(crit.getValue(), docValueClass);
                            meets = docValue != null && ((Comparable)docValue).compareTo(convertedCriteriaValue) < 0;
                            break;
                        case ComparisonOperator.GTE:
                            convertedCriteriaValue = ObjectConverter.convert(crit.getValue(), docValueClass);
                            meets = docValue != null && ((Comparable)docValue).compareTo(convertedCriteriaValue) >= 0;
                            break;
                        case ComparisonOperator.LTE:
                            convertedCriteriaValue = ObjectConverter.convert(crit.getValue(), docValueClass);
                            meets = docValue != null && ((Comparable)docValue).compareTo(convertedCriteriaValue) <= 0;
                            break;
                        case ComparisonOperator.NE:
                            convertedCriteriaValue = ObjectConverter.convert(crit.getValue(), docValueClass);
                            meets = !((docValueExists && docValue == null && crit.getValue() == null) || (convertedCriteriaValue != null && convertedCriteriaValue.equals(docValue)));
                            break;
                        case ComparisonOperator.IN:
                            //Note: Does not support mixed value type lists for the criteria
                            boolean isIn = false;
                            Object convetredCriteriaInValue;
                            for(Object critInValue : (Collection)crit.getValue()){
                                convetredCriteriaInValue = ObjectConverter.convert(critInValue, docValueClass);
                                if((docValueExists && docValue == null && critInValue == null) || (docValue !=  null && convetredCriteriaInValue != null && convetredCriteriaInValue.equals(docValue))){
                                    isIn = true;
                                    break;
                                }
                            }
                            meets = isIn;
                            break;
                        case ComparisonOperator.ALL:
                            boolean allAreIn = true;
                            Object convetredCriteriaAllValue;
                            for(Object critAllValue : (Collection)crit.getValue()){
                                convetredCriteriaAllValue = ObjectConverter.convert(critAllValue, docValueClass);
                                if(!((docValueExists && docValue == null && critAllValue == null) || (docValue != null && convetredCriteriaAllValue != null && ((Collection)docValue).contains(convetredCriteriaAllValue)))){
                                    allAreIn = false;
                                    break;
                                }
                            }
                            meets = allAreIn;
                            break;
                        case ComparisonOperator.NIN:
                            boolean notIn = true;
                            Object convertedCriteriaNotInValue;
                            for(Object critNotInValue : (Collection)crit.getValue()){
                                convertedCriteriaNotInValue = ObjectConverter.convert(critNotInValue, docValueClass);
                                if((docValueExists && docValue == null && critNotInValue == null) || (convertedCriteriaNotInValue != null && convertedCriteriaNotInValue.equals(docValue))){
                                    notIn = false;
                                    break;
                                }
                            }
                            meets = notIn;
                            break;
                        case ComparisonOperator.LIKE:
                            if (crit.getValue() == null){
                                meets = false;
                            }else if (!String.class.isAssignableFrom(crit.getValue().getClass())){
                                return false;
                            }else{
                                Pattern p = Pattern.compile((String)crit.getValue());
                                Matcher m = p.matcher((String)docValue);
                                meets = m.matches();
                            }
                            break;
                        case ComparisonOperator.EXISTS:
                            if (Boolean.class.isAssignableFrom(crit.getValue().getClass())){
                                meets = (boolean)crit.getValue() == docValueExists;
                            }else{
                                throw new IllegalArgumentException("You must specify true or false for exists.");
                            }
                            break;
                        case ComparisonOperator.MATCH:
                            if (docValueExists && docValue != null && Collection.class.isAssignableFrom(docValue.getClass())){
                                meets = false;
                                SubscriptionFiqlParser parser = new SubscriptionFiqlParser();
                                SubscriptionCriteria subCriteria = parser.parse((String)crit.getValue());
                                for(DotNotationMap subDoc : (Collection<DotNotationMap>)docValue){
                                    if (subCriteria.meetsCriteria(subDoc)){
                                        meets = true;
                                        break;
                                    }
                                }
                            }
                            break;
                        default:
                            throw new RuntimeException("The comparison operator " + crit.getKey() + " is not supported.");
                    }
                    //This is here for a reason. I don't remember why though
                    if (meets == false){
                        return false;
                    }
                }
                //All criteria in the criteria set were true
                return true;
            }
        }
    }
    
    /**
     * Sets the property name of the criteria and returns the new SubscriptionCriteria
     * object to use.
     * 
     * @param propertyName The name of the property
     * @return The new criteria object
     */
    public static SubscriptionCriteria where(String propertyName){
        return new SubscriptionCriteria(propertyName);
    }
    
    /**
     * Adds the list of criteria to an AND list where each criteria must be true
     * for the condition to be met.
     * 
     * @param c The list of criteria
     * @return The new object to use
     */
    public SubscriptionCriteria and(SubscriptionCriteria... c){
        SubscriptionCriteria andCriteria = new SubscriptionCriteria(LogicalOperator.AND).is(Arrays.asList(c));
        chain.add(andCriteria);
        return this;
    }
    
    /**
     * Adds the list of criteria to an AND list where each criteria must be true
     * for the condition to be met.
     * 
     * @param c The list of criteria
     * @return The new object to use
     */
    public SubscriptionCriteria and(List<SubscriptionCriteria> c){
        SubscriptionCriteria andCriteria = new SubscriptionCriteria(LogicalOperator.AND).is(c);
        chain.add(andCriteria);
        return this;
    }
    
    /**
     * Adds the list of criteria to an OR list one of the criteria must be true
     * for the condition to be met.
     * 
     * @param c The list of criteria
     * @return The new object to use
     */
    public SubscriptionCriteria or(SubscriptionCriteria... c){
        SubscriptionCriteria orCriteria = new SubscriptionCriteria(LogicalOperator.OR).is(Arrays.asList(c));
        chain.add(orCriteria);
        return this;
    }
    
    /**
     * Adds the list of criteria to an OR list one of the criteria must be true
     * for the condition to be met.
     * 
     * @param c The list of criteria
     * @return The new object to use
     */
    public SubscriptionCriteria or(List<SubscriptionCriteria> c){
        SubscriptionCriteria orCriteria = new SubscriptionCriteria(LogicalOperator.OR).is(c);
        chain.add(orCriteria);
        return this;
    }
    
    /**
     * Sets the value for this criteria to be compared against.
     * <p>
     * The property name be set first with the constructor, the where method,
     * or the setPropertyName method.
     * 
     * @param value The value to compare
     * @return The resulting criteria object
     */
    public SubscriptionCriteria is(Object value){
        this.value = value;
        return this;
    }
    
    /**
     * Makes this criteria a less than comparison with the value.
     * 
     * @param value The value to compare
     * @return The criteria object to use
     */
    public SubscriptionCriteria lt(Object value){
        criteria.put(ComparisonOperator.LT, value);
        return this;
    }
    
    /**
     * Makes this criteria a greater than than comparison with the value.
     * 
     * @param value The value to compare
     * @return The criteria object to use
     */
    public SubscriptionCriteria gt(Object value){
        criteria.put(ComparisonOperator.GT, value);
        return this;
    }
    
    /**
     * Makes this criteria a less than or equal comparison with the value.
     * 
     * @param value The value to compare
     * @return The criteria object to use
     */
    public SubscriptionCriteria lte(Object value){
        criteria.put(ComparisonOperator.LTE, value);
        return this;
    }
    
    /**
     * Makes this criteria a greater than or equal comparison with the value.
     * 
     * @param value The value to compare
     * @return The criteria object to use
     */
    public SubscriptionCriteria gte(Object value){
        criteria.put(ComparisonOperator.GTE, value);
        return this;
    }
    
    /**
     * Makes this criteria a not equal comparison with the value.
     * 
     * @param value The value to compare
     * @return The criteria object to use
     */
    public SubscriptionCriteria ne(Object value){
        criteria.put(ComparisonOperator.NE, value);
        return this;
    }
    
    /**
     * Assuming that the property is an array, will determine if the value is 
     * contained in the array.
     * 
     * @param value The value to compare
     * @return The criteria object to use
     */
    public SubscriptionCriteria in(Object value){
        if (!Collection.class.isAssignableFrom(value.getClass())){
            HashSet newValue = new HashSet();
            newValue.add(value);
            criteria.put(ComparisonOperator.IN, newValue);
        }else{
            criteria.put(ComparisonOperator.IN, value);
        }
        return this;
    }
    
    /**
     * Compares the provided list against the property, which may or may not be an
     * array, and determines if all of the values in the provided list are contained
     * in the property.
     * 
     * @param value The value to compare
     * @return The resulting criteria object
     */
    public SubscriptionCriteria all(Object value){
        if (!Collection.class.isAssignableFrom(value.getClass())){
            HashSet newValue = new HashSet();
            newValue.add(value);
            criteria.put(ComparisonOperator.ALL, newValue);
        }else{
            criteria.put(ComparisonOperator.ALL, value);
        }
        return this;
    }
    
    /**
     * Makes this criteria a not in comparison where the value is not contained
     * in the property value.
     * 
     * @param value The value to compare
     * @return The criteria object to use
     */
    public SubscriptionCriteria nin(Object value){
        if (!Collection.class.isAssignableFrom(value.getClass())){
            HashSet newValue = new HashSet();
            newValue.add(value);
            criteria.put(ComparisonOperator.NIN, newValue);
        }else{
            criteria.put(ComparisonOperator.NIN, value);
        }
        return this;
    }
    
    /**
     * Compares the value of the property using the provided regex string.
     * 
     * @param value The regex string
     * @return The criteria object to use
     */
    public SubscriptionCriteria regex(Object value){
        criteria.put(ComparisonOperator.LIKE, value);
        return this;
    }
    
    /**
     * Used to determine if a property exists.
     * 
     * @param value True if the property should exist and false otherwise
     * @return The criteria object to use
     */
    public SubscriptionCriteria exists(Boolean value){
        criteria.put(ComparisonOperator.EXISTS, value);
        return this;
    }
    
    /**
     * A special comparison that will compare a list of objects with the
     * sub FIQL query provided.
     * 
     * @param subQuery The sub fiql Query
     * @return The criteria object to use
     */
    public SubscriptionCriteria matches(String subQuery){
        criteria.put(ComparisonOperator.MATCH, subQuery);
        return this;
    }
    
}
