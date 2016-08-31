package cn.guoyukun.es;

import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.data.domain.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by guoyukun on 2016/6/22.
 */
public class ElasticsearchUtil {

    public static void refresh(Client client, String indexName) {
        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
    }

    // ========================

    public static boolean insert(Client client, String indexName, String typeName, String json) {
        return insert(client, indexName, typeName, json, null, Boolean.FALSE);
    }

    public static boolean insert(Client client, String indexName, String typeName, String json, boolean refresh) {
        return insert(client, indexName, typeName, json, null, refresh);
    }

    public static boolean insert(Client client, String indexName, String typeName, String json, String id) {
        return insert(client, indexName, typeName, json, id, Boolean.FALSE);
    }

    public static boolean insert(Client client, String indexName, String typeName, String json, String id, boolean refresh) {
        return insert(client, indexName, typeName, json, id, null, refresh);
    }

    public static boolean insert(Client client, String indexName, String typeName, String json, String id, String routing, boolean refresh) {
        insert(client, indexName, typeName, (Object)json, id, routing, refresh);
        return true;
    }

    // ============================
    public static boolean insert(Client client, String indexName, String typeName, Map data) {
        return insert(client, indexName, typeName, data, null, Boolean.FALSE);
    }

    public static boolean insert(Client client, String indexName, String typeName, Map data, boolean refresh) {
        return insert(client, indexName, typeName, data, null, refresh);
    }

    public static boolean insert(Client client, String indexName, String typeName, Map data, String id) {
        return insert(client, indexName, typeName, data, id, Boolean.FALSE);
    }

    public static boolean insert(Client client, String indexName, String typeName, Map data, String id, boolean refresh) {
        return insert(client, indexName, typeName, data, id, null, refresh);
    }
    public static boolean insert(Client client, String indexName, String typeName, Map data, String id, String routing, boolean refresh) {
        String eId = insert(client, indexName, typeName, (Object)data, id, routing, refresh);
        return StringUtils.isNotBlank(eId);
    }

    public static boolean bulk(Client client, String indexName, String typeName, List<Map<String, Object>> dataList, String idKey, String routingKey, boolean refresh) {
        return bulk(client, indexName, typeName, dataList, idKey, routingKey, refresh, Boolean.FALSE);

    }

    public static boolean bulk(Client client, String indexName, String typeName, List<Map<String, Object>> dataList, String idKey, String routingKey, boolean refresh, boolean removeRoutingKey) {
        BulkRequestBuilder builder = client.prepareBulk();
        for(Map data : dataList){
            Object id = data.get(idKey);
            if(id==null){
                continue;
            }
            String routing = data.get(routingKey) != null ? data.get(routingKey).toString() : null;

            if(removeRoutingKey){
                data.remove(routingKey);
            }
            IndexRequestBuilder index = indexBuilder(client, indexName, typeName, data, id.toString(), routing, Boolean.FALSE);
            builder.add(index);
        }
        builder.setRefresh(refresh).execute().actionGet();
        return true;
    }

    // ===========================
    private static String insert(Client client, String indexName, String typeName, Object data, String id, String routing, boolean refresh){
        IndexRequestBuilder builder = indexBuilder(client, indexName, typeName, data, id, routing, refresh);
        IndexResponse response = builder.execute().actionGet();
        return response.getId();
    }
    private static IndexRequestBuilder indexBuilder(Client client, String indexName, String typeName, Object data, String id, String routing, boolean refresh){
        IndexRequestBuilder builder = client.prepareIndex(indexName, typeName, id);
        if(StringUtils.isNotBlank(routing)){
            builder.setRouting(routing);
        }
        if (refresh) {
            builder.setRefresh(true);
        }
        if(data instanceof String){
            builder.setSource((String) data);
        }
        if(data instanceof Map){
            builder.setSource((Map)data);
        }
        return builder;
    }

    // ====================
    public static boolean update(Client client, String indexName, String typeName, Map data, String id) {
        return update(client, indexName, typeName, data, id, Boolean.FALSE);
    }

    private static boolean update(Client client, String indexName, String typeName, Map data, String id, Boolean refresh) {
        client.prepareUpdate(indexName, typeName, id)
                .setDoc(data)
                .execute()
                .actionGet();
        if (refresh) {
            refresh(client, indexName);
        }
        return true;
    }

    // =======================================

    public static void deleteAllBySlowLoop(Client client, String indexName, String typeName) {
        deleteAllBySlowLoop(client, indexName, typeName, Boolean.FALSE);
    }

    public static void deleteAllBySlowLoop(Client client, String indexName, String typeName, boolean refresh) {
        int pageSize = 100;
        int page = 0;
        while (true) {
            Pageable pageable = new PageRequest(page, pageSize);
            SearchResponse resp = doSearch(client, indexName, typeName, QueryBuilders.matchAllQuery(), null, pageable);
            SearchHits hits = resp.getHits();
            SearchHit[] hitArray = hits.getHits();
            if (hitArray.length < 1) {
                break;
            }
            for (SearchHit hit : hitArray) {
                String id = hit.getId();
                delete(client, indexName, typeName, id);
            }
            page++;
        }
        if (refresh) {
            refresh(client, indexName);
        }
    }

    public static void delete(Client client, String indexName, String typeName, String id) {
        DeleteRequestBuilder drb = client.prepareDelete(indexName, typeName, id);

        DeleteResponse response = drb.execute().actionGet();
        response.getId();

    }

    // =======================================
    public static SearchResponse doSearch(Client client, String indexName, String typeName, EsQuery query, Pageable pageable) {
        return doSearch(client, indexName, typeName, query.buildQuery(), query.getHighlights(), pageable);
    }

    public static SearchResponse doSearch(Client client, String indexName, String typeName, QueryBuilder qb, Pageable pageable) {
        return doSearch(client, indexName, typeName, qb, null, pageable);
    }

    public static SearchResponse doSearch(Client client, String indexName, String typeName, QueryBuilder qb, String[] highlights, Pageable pageable) {
        SearchRequestBuilder srb =
                client.prepareSearch(indexName)
                        .setTypes(typeName)
                        // query_then_fetch是先查到相关结构，然后聚合不同node上的结果后排序
                        .setSearchType(SearchType.QUERY_THEN_FETCH)
                        // 查询的termName和termvalue
                        .setQuery(qb);
        if(highlights!=null && highlights.length>0){
            for (String highlight: highlights){
                srb.addHighlightedField(highlight);
            }
        }
        if (pageable != null) {
            // 设置分页
            srb.setFrom(pageable.getOffset()).setSize(pageable.getPageSize());
            // 设置排序field
            Sort sort = pageable.getSort();
            if (sort != null) {
                Iterator<Sort.Order> ite = sort.iterator();
                while (ite.hasNext()) {
                    Sort.Order order = ite.next();
                    srb.addSort(order.getProperty(),
                            order.getDirection() == Sort.Direction.ASC ? SortOrder.ASC : SortOrder.DESC);
                }
            }
        }

        SearchResponse sResponse = srb.execute().actionGet();
        return sResponse;
    }

    // =======================================
    public static <T> Page<T> doSearchAndConvert(Client client, String indexName, String typeName, EsQuery query, Pageable pageable, ResultConverter<T> rc) {
        return doSearchAndConvert(client, indexName, typeName, query.buildQuery(), query.getHighlights(), pageable, rc);
    }

    public static <T> Page<T> doSearchAndConvert(Client client, String indexName, String typeName, QueryBuilder qb, Pageable pageable, ResultConverter<T> rc) {
        return doSearchAndConvert(client, indexName, typeName, qb, null, pageable, rc);
    }

    public static <T> Page<T> doSearchAndConvert(Client client, String indexName, String typeName, QueryBuilder qb, String[] highlights, Pageable pageable, ResultConverter<T> rc) {
        SearchResponse sResponse = doSearch(client, indexName, typeName, qb, highlights, pageable);
        SearchHits hits = sResponse.getHits();

        List<T> list = Lists.newArrayListWithExpectedSize((int) hits.getTotalHits());

        if (hits.getHits() == null) {
            return new PageImpl<T>(list, pageable, hits.getTotalHits());
        }

        for (SearchHit hit : hits.getHits()) {
            T obj = rc.convert(hit);
            list.add(obj);
        }
        return new PageImpl<T>(list, pageable, hits.getTotalHits());
    }

    // =======================================

    public static Page<Map> searchForMap(Client client, String indexName, String typeName, QueryBuilder qb, Pageable pageable) {
        return doSearchAndConvert(client, indexName, typeName, qb, pageable, new ResultConverter<Map>() {
            @Override
            public Map convert(SearchHit hit) {
                return hit.getSource();
            }
        });
    }

    public static <T> Page<T> searchForObj(Client client, String indexName, String typeName, final QueryBuilder qb, String[] highlights, Pageable pageable, final Class<T> t) {
        return doSearchAndConvert(client, indexName, typeName, qb, highlights, pageable, new ResultConverter<T>() {
            @Override
            public T convert(SearchHit hit) {
                T obj = JsonUtil.fromJson(hit.getSourceAsString(), t);
                Map<String, HighlightField> result = hit.highlightFields();
                for(Map.Entry<String, HighlightField> entry: result.entrySet()){
                    String fieldName = entry.getKey();
                    HighlightField field = entry.getValue();
                    if(field!=null){
                        Text[] texts =  field.fragments();
                        try {
                            BeanUtils.setProperty(obj, fieldName, texts[0].string());
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        }
                    }
                }
                //}
                return obj;
            }
        });
    }

    public static <T> Page<T> searchForObj(Client client, String indexName, String typeName, final QueryBuilder qb, Pageable pageable, final Class<T> t) {
        return searchForObj(client, indexName, typeName, qb, null, pageable, t);
    }

    public static <T> Page<T> searchForObj(Client client, String indexName, String typeName, final EsQuery query, Pageable pageable, final Class<T> t) {
        return searchForObj(client, indexName, typeName, query.buildQuery(), query.getHighlights(), pageable, t);
    }

    //====================================

    public static Map<String, Object> findForMapById(Client client, String indexName, String typeName, String id) {
        GetResponse resp= client.prepareGet(indexName, typeName, id).execute().actionGet();
        if(resp.isExists()){
            return resp.getSource();
        }else{
            return null;
        }
    }

    public static <T> T findById(Client client, String indexName, String typeName, String id, final Class<T> t) {
        GetResponse resp= client.prepareGet(indexName, typeName, id).execute().actionGet();
        if(resp.isExists()){
            return JsonUtil.fromJson(resp.getSourceAsString(), t);
        }else{
            return null;
        }
    }

    public interface ResultConverter<T> {
        <T> T convert(SearchHit hit);
    }
}
