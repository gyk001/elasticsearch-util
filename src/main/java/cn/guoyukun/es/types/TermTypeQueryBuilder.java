package cn.guoyukun.es.types;

import cn.guoyukun.es.EsTypeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Created by guoyukun on 2016/7/11.
 */
public class TermTypeQueryBuilder implements EsTypeQueryBuilder {
    @Override
    public QueryBuilder build(String prop, Object value) {
        return QueryBuilders.termQuery(prop, value);
    }
}
