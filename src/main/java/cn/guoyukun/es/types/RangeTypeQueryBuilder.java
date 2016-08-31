package cn.guoyukun.es.types;

import cn.guoyukun.es.EsQuery;
import cn.guoyukun.es.EsTypeQueryBuilder;
import cn.guoyukun.es.EsQueryUtil;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

/**
 * Created by guoyukun on 2016/7/11.
 */
public class RangeTypeQueryBuilder implements EsTypeQueryBuilder {

    @Override
    public QueryBuilder build(String prop, Object value) {
        if(value instanceof EsQuery.Range){
            EsQuery.Range range = (EsQuery.Range) value;
            RangeQueryBuilder builder = QueryBuilders.rangeQuery(prop);
            boolean vaild = false;
            if( ! EsQueryUtil.isEmpty(range.getGt())){
                builder.gt(range.getGt());
                vaild = true;
            }
            if(! EsQueryUtil.isEmpty(range.getGte())){
                builder.gte(range.getGte());
                vaild = true;
            }
            if(! EsQueryUtil.isEmpty(range.getLt())){
                builder.lt(range.getLt());
                vaild = true;
            }
            if(! EsQueryUtil.isEmpty(range.getLte())){
                builder.lte(range.getLte());
                vaild = true;
            }
            if(vaild){
                if(StringUtils.isNotBlank(range.getFormat())){
                    builder.format(range.getFormat());
                }
                if(StringUtils.isNotBlank(range.getTimeZone())){
                    builder.timeZone(range.getTimeZone());
                }
                return builder;
            }

        }
        return null;
    }


}
