package cn.guoyukun.es.types;

import cn.guoyukun.es.EsTypeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Collection;

/**
 * Created by guoyukun on 2016/7/11.
 */
public class TermsTypeQueryBuilder implements EsTypeQueryBuilder {
    @Override
    public QueryBuilder build(String prop, Object value) {
        if(value instanceof Object[]){
            Object[] arr = (Object[]) value;
            if(arr.length>0){
                return QueryBuilders.termsQuery(prop, arr);
            }
        }else if(value instanceof Collection){
            if( ! ((Collection) value).isEmpty()){
                return QueryBuilders.termsQuery(prop, (Collection) value);
            }
        }
        return null;
    }
}
