package org.datakow.core.components;

import java.io.InputStream;
import java.util.Iterator;

/**
 * Class used to convert an Iterator or strings to an input stream. 
 * <p>
 * During the conversion it is also possible to supply a prefix, suffix, and
 * a separator string.
 * 
 * @author kevin.off
 */
public class IteratorToInputStream extends InputStream {

        private Iterator iterator;
        private byte[] buf = null;
        private int pos = 0;
        private int count = 0;
        
        protected String prefix = "";
        protected String separator = "";
        protected String suffix = "";
        protected boolean quotedStrings = false;
        protected boolean isJsonProducer = false;
        
        /**
         * Creates an instance capable of converting the iterator into a Stream containing
         * a valid JSON array of the elements in the iterator
         * 
         * @param strings The iterator to convert
         * @return The input stream
         */
        public static IteratorToInputStream iteratorToJsonArrayInputStream(Iterator<String> strings){
            return new IteratorToInputStream(strings, "[", ",", "]", true);
        }
        
        public static IteratorToInputStream jsonObjectIteratorToJsonArrayInputStream(Iterator<String> strings){
            return new IteratorToInputStream(strings, "[", ",", "]", false);
        }
        
        public static IteratorToInputStream jsonProducerIteratorToJsonArrayInputStream(Iterator<? extends JsonProducer> objects){
            IteratorToInputStream stream = new IteratorToInputStream(objects, "[", ",", "]", false);
            stream.isJsonProducer = true;
            return stream;
        }
        
        /**
         * Creates a new instance with all required fields. 
         * 
         * @param strings The iterator to convert
         * @param prefix A value to prefix the stream with
         * @param separator A separator to use in between each value
         * @param suffix A value to add to the end of the stream
         * @param quotedStrings True if the values in the iterator should be surrounded by quotes
         */
        private IteratorToInputStream(Iterator strings, String prefix, String separator, String suffix, boolean quotedStrings){
            this.iterator = strings;
            this.prefix = prefix;
            this.separator = separator;
            this.suffix = suffix;
            this.quotedStrings = quotedStrings;
        }
        
        protected IteratorToInputStream(Iterator strings, String prefix, String separator, String suffix, boolean quotedStrings, boolean isJsonProducer){
            this.iterator = strings;
            this.prefix = prefix;
            this.separator = separator;
            this.suffix = suffix;
            this.quotedStrings = quotedStrings;
            this.isJsonProducer = true;
        }
        
        @Override
        public int read() {
            reloadIfNeeded();
            return (pos < count) ? (buf[pos++] & 0xff) : -1;
        }
        
        /**
         * Checks to see if we are out of bytes to read on the current item
         * gathered from the iterator. 
         * If we are out of bytes then the next item is loaded into the buffer.
         */
        private void reloadIfNeeded(){
            StringBuilder tmp;
            if (buf == null && prefix != null && !prefix.isEmpty()){
                initBuffer(prefix);
            }else if (pos >= count){
                if (iterator.hasNext()){
                    if (quotedStrings){
                        tmp = new StringBuilder("\"" + getNext(this.iterator) + "\"");
                    }else{
                        if (isJsonProducer){
                            try{
                                tmp =  new StringBuilder(((JsonProducer)getNext(iterator)).toJson());
                            }catch(Exception ex){
                                throw new IllegalArgumentException("Problem converting object to JSON", ex);
                            }
                        }else{
                            tmp = new StringBuilder((String)getNext(iterator));
                        }
                        
                    }
                    if (iterator.hasNext()){
                        tmp.append(this.separator);
                    }else{
                        tmp.append(this.suffix);
                    }
                    initBuffer(tmp.toString());
                }
            }
        }

        protected Object getNext(Iterator it){
            return it.next();
        }
        
        /**
         * Initialize the byte buffer to read from.
         * 
         * @param s The string to initialize the buffer with
         */
        private void initBuffer(String s){
            buf = s.getBytes();
            count = buf.length;
            pos = 0;
        }
        
    }
