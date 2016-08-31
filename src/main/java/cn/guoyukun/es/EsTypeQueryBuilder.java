package cn.guoyukun.es;

import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by guoyukun on 2016/7/11.
 */
public interface EsTypeQueryBuilder {
    QueryBuilder build(String prop, Object value);
}
