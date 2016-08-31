package cn.guoyukun.es;

import cn.guoyukun.es.types.MatchTypeQueryBuilder;
import cn.guoyukun.es.types.RangeTypeQueryBuilder;
import cn.guoyukun.es.types.TermTypeQueryBuilder;
import cn.guoyukun.es.types.TermsTypeQueryBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by guoyukun on 2016/7/11.
 */
public class EsQueryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(EsQueryUtil.class);

    public static final Map<String, EsTypeQueryBuilder> builders = Maps.newHashMap();

    static {
        builders.put("match", new MatchTypeQueryBuilder());
        builders.put("range", new RangeTypeQueryBuilder());
        builders.put("terms", new TermsTypeQueryBuilder());
        builders.put("term", new TermTypeQueryBuilder());
    }

    public static List<QueryBuilder> buildFilterList(Map<String, Object> queryMap){
        List<QueryBuilder> filters = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : queryMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (isEmpty(key) || isEmpty(value)) {
                continue;
            }

            String type;
            String prop;

            int _index = key.lastIndexOf('_');
            if(_index==-1){
                type = "match";
                prop = key;
            }else{
                type = key.substring(_index+1);
                prop = key.substring(0, _index);
            }

            EsTypeQueryBuilder builder = builders.get(type);
            if(builder==null){
                throw new IllegalArgumentException("不支持的查询类型："+type);
            }
            QueryBuilder qb = builder.build(prop, value);
            if(qb!=null){
                filters.add(qb);
            }
        }

        return filters;
    }

    public static QueryBuilder buildQuery(List<QueryBuilder> filters){
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        bqb.must(QueryBuilders.matchAllQuery());
        for(QueryBuilder filter: filters){
            bqb.filter(filter);
        }
        return bqb;
    }

    public static QueryBuilder buildQuery(Map<String, Object> queryMap){
        List<QueryBuilder> filters = buildFilterList(queryMap);
        return buildQuery(filters);
    }

    public static boolean isEmpty(Collection value){
        return value ==null || value.isEmpty();
    }

    public static boolean isEmpty(Object[] value){
        return value ==null || value.length<1;
    }

    public static boolean isEmpty(String value){
        return value ==null || value.isEmpty();
    }

    public static boolean isEmpty(Object value){
        return value ==null ||
                (value instanceof String && "".equals(value)) ||
                (value instanceof Object[] && ((Object[]) value).length<1) ||
                (value instanceof Collection && ((Collection) value).isEmpty());
    }
}
