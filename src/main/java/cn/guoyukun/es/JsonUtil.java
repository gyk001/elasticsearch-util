package cn.guoyukun.es;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by guoyukun on 2016/6/23.
 */
public class JsonUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JsonUtil.class);
    private static ObjectMapper MAPPER;

    static{
        MAPPER = new ObjectMapper();
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        //MAPPER.getSerializationConfig().addMixInAnnotations(BaseShardModel.class, ShardModuleMixIn.class);
        //MAPPER.getDeserializationConfig().addMixInAnnotations(BaseShardModel.class, ShardModuleMixIn.class);
    }

    public static String toJson(Object obj){
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            LOG.error("生成Json异常", obj, e);
            return null;
        }
    }

    public static <T> T fromJson(String json, Class<T> t){
        try {
            return MAPPER.readValue(json, t);
        } catch (IOException e) {
            LOG.error("解析Json异常", json, e);
            return null;
        }
    }

    public static <T> T fromJson(String json, TypeReference<T> t){
        try {
            return MAPPER.readValue(json, t);
        } catch (IOException e) {
            LOG.error("解析Json异常", json, e);
            return null;
        }
    }


    static abstract class ShardModuleMixIn {
        @JsonIgnore abstract boolean isForceRead2Writer();
        @JsonIgnore abstract Map<String, String> getTableNames();

    }
}
