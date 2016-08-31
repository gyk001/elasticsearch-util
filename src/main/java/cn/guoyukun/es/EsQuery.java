package cn.guoyukun.es;

import com.google.common.collect.Maps;
import jodd.bean.BeanCopy;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by guoyukun on 2016/7/7.
 */
public class EsQuery {
    private static final Logger LOG = LoggerFactory.getLogger(EsQuery.class);

    private String[] highlights;


    public QueryBuilder buildQuery(){
        try {
            Map<String, Object> queryMap = Maps.newHashMap();
            BeanCopy.from(this).toMap(queryMap).exclude("highlights").copy();
            List<QueryBuilder> filters = EsQueryUtil.buildFilterList(queryMap);
            beforeBuild(filters);
            return EsQueryUtil.buildQuery(filters);
        }catch (Exception e){
            LOG.error("", e);
            return QueryBuilders.matchAllQuery();
        }
    }
    protected List<QueryBuilder> beforeBuild(List<QueryBuilder> filters){
        return filters;
    }
    protected Map<String, Object> beforeFilter(Map<String,Object> queryMap){
        return queryMap;
    }

//    static public class Highlight {
//        private String[] preTags;
//        private String[] postTags;
//        private Map<String, HighlightField> fields;
//
//        public String[] getPreTags() {
//            return preTags;
//        }
//
//        public void setPreTags(String[] preTags) {
//            this.preTags = preTags;
//        }
//
//        public String[] getPostTags() {
//            return postTags;
//        }
//
//        public void setPostTags(String[] postTags) {
//            this.postTags = postTags;
//        }
//
//        public Map<String, HighlightField> getFields() {
//            return fields;
//        }
//
//        public void setFields(Map<String, HighlightField> fields) {
//            this.fields = fields;
//        }
//    }
//
//    static public class HighlightField{
//        //plain, postings ， fvh
//        private String type;
//        private Integer fragmentSize;
//
//        public String getType() {
//            return type;
//        }
//
//        public void setType(String type) {
//            this.type = type;
//        }
//
//        public Integer getFragmentSize() {
//            return fragmentSize;
//        }
//
//        public void setFragmentSize(Integer fragmentSize) {
//            this.fragmentSize = fragmentSize;
//        }
//    }

    static public class Range{
        Object gte;
        Object gt;
        Object lte;
        Object lt;
        String format;
        String timeZone;

        public Object getGte() {
            return gte;
        }

        public void setGte(Object gte) {
            this.gte = gte;
        }

        public Object getGt() {
            return gt;
        }

        public void setGt(Object gt) {
            this.gt = gt;
        }

        public Object getLte() {
            return lte;
        }

        public void setLte(Object lte) {
            this.lte = lte;
        }

        public Object getLt() {
            return lt;
        }

        public void setLt(Object lt) {
            this.lt = lt;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }

        public String getTimeZone() {
            return timeZone;
        }

        public void setTimeZone(String timeZone) {
            this.timeZone = timeZone;
        }
    }

    public String[] getHighlights() {
        return highlights;
    }

    public void setHighlights(String[] highlights) {
        this.highlights = highlights;
    }
}
