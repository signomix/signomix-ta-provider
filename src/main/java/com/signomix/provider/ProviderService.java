package com.signomix.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.signomix.common.db.AuthDao;
import com.signomix.common.db.AuthDaoIface;
import com.signomix.common.db.DataQuery;
import com.signomix.common.db.DataQueryException;
import com.signomix.common.db.IotDatabaseDao;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.ChannelData;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.cache.CacheResult;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class ProviderService {
    private static final Logger LOG = Logger.getLogger(ProviderService.class);

    // TODO: test /q/health/ready

    @Inject
    @DataSource("iot")
    AgroalDataSource ds;

    @Inject
    @DataSource("auth")
    AgroalDataSource ds2;

    IotDatabaseIface dataDao;
    AuthDaoIface authDao;

    @ConfigProperty(name = "signomix.app.key", defaultValue = "not_configured")
    String appKey;
    @ConfigProperty(name = "signomix.core.host", defaultValue = "not_configured")
    String coreHost;
    @ConfigProperty(name = "signomix.query.limit", defaultValue = "500")
    String requestLimitConfigured;

    public void onApplicationStart(@Observes StartupEvent event) {
        dataDao = new IotDatabaseDao();
        dataDao.setDatasource(ds);
        authDao = new AuthDao();
        authDao.setDatasource(ds2);
        try {
            LOG.info("requestLimitConfigured:" + requestLimitConfigured);
            int requestLimit = Integer.parseInt(requestLimitConfigured);
            dataDao.setQueryResultsLimit(requestLimit);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    @CacheResult(cacheName = "token-cache")
    String getUserID(String sessionToken) {
        LOG.debug("token:" + sessionToken);
        return authDao.getUser(sessionToken);
    }

    @CacheResult(cacheName = "query-cache")
    List getData(String userID, String deviceEUI, String channelName, String query) {
        LOG.debug("userID:" + userID);
        LOG.debug("device:" + deviceEUI);
        LOG.debug("channel:" + channelName);
        LOG.debug("query:" + query);
        try {
            if (channelName != null && !"$".equals(channelName)) {
                ArrayList result = (ArrayList) dataDao.getValues(userID, deviceEUI,
                        query + " channel " + channelName);
                return normalize(result, query + " channel " + channelName);
            } else {
                return normalize((ArrayList) dataDao.getValues(userID, deviceEUI, query), query);
            }
        } catch (IotDatabaseException ex) {
            LOG.warn(ex.getMessage());
            return new ArrayList();
        }

    }

    List getGroupData(String userID, String groupEUI, String channelNames) {
        LOG.debug("group:" + groupEUI);
        LOG.debug("channel:" + channelNames);

        long organizationId = -1;
        long secondsBack = 3600;
        // LOG.debug("query:" + query);
        try {
            // if (channelName != null && !"$".equals(channelName)) {
            // String query2=null!=query?query:"";
            // query2=query2+" channel " + channelName;
            // ArrayList result = (ArrayList) dataDao.getGroupValues(userID,
            // groupEUI,query2);
            // return new ArrayList<>();
            // } else {
            // return (ArrayList) dataDao.getGroupValues(userID, groupEUI, channelNames);
            return dataDao.getValuesOfGroup(userID, organizationId, groupEUI, channelNames, secondsBack);
            // }
        } catch (IotDatabaseException ex) {
            LOG.warn(ex.getMessage());
            return new ArrayList();
        }

    }

    /**
     * Fills missing measurements with null values.
     * 
     * @param data
     * @return
     */
    private ArrayList normalize(ArrayList data, String query) {
        DataQuery dq;
        try {
            dq = DataQuery.parse(query);
        } catch (DataQueryException ex) {
            ex.printStackTrace();
            return data;
        }
        List<String> channelNames = dq.getChannels();
        HashMap<String, SortedMap<Long, ChannelData>> map = new HashMap<>();
        ArrayList<ArrayList> result = new ArrayList<>();
        ArrayList<ChannelData> subList;
        ChannelData tmpCd;
        int numberOfLists = data.size();
        if (channelNames.size() < 2) {
            return data;
        }
        SortedMap<Long, ChannelData> subMap;
        for (int i = 0; i < numberOfLists; i++) {
            LOG.info("i==" + i);
            subList = (ArrayList<ChannelData>) data.get(i);
            for (int j = 0; j < subList.size(); j++) {
                LOG.info("j==" + j);
                tmpCd = subList.get(j);
                subMap = map.get(tmpCd.getName());
                if (null == subMap) {
                    subMap = new TreeMap<>();
                }
                subMap.put(tmpCd.getTimestamp(), tmpCd);
                map.put(tmpCd.getName(), subMap);
            }
        }

        String mainKey, key;
        Iterator<Long> it;
        SortedMap<Long, ChannelData> submap2, tmpMap;
        Long timestamp;
        for (int i = 0; i < channelNames.size(); i++) {
            mainKey = (String) channelNames.get(i);
            submap2 = map.get(mainKey);
            if (null != submap2) {
                it = submap2.keySet().iterator();
                while (it.hasNext()) {
                    timestamp = it.next();
                    for (int j = 0; j < channelNames.size(); j++) {
                        key = (String) channelNames.get(j);
                        if (!key.equals(mainKey)) {
                            tmpMap = map.get(key);
                            if (null != tmpMap && null == tmpMap.get(timestamp)) {
                                tmpCd = submap2.get(timestamp);
                                tmpMap.put(timestamp, new ChannelData(tmpCd.getDeviceEUI(), key, null, timestamp));
                            }
                        }
                    }
                }
            }
        }
        ArrayList<Long> timestamps = new ArrayList<>();
        SortedMap<Long, ChannelData> tmpMap2 = map.get(channelNames.get(0));
        if (null != tmpMap2) {
            for (Map.Entry mapElement : tmpMap2.entrySet()) {
                timestamps.add((Long) mapElement.getKey());
            }
        }
        for (int i = 0; i < timestamps.size(); i++) {
            subList = new ArrayList<>();
            for (int j = 0; j < channelNames.size(); j++) {
                SortedMap<Long, ChannelData> tmpMap3=map.get(channelNames.get(j));
                if(null!=tmpMap3){
                    subList.add(tmpMap3.get(timestamps.get(i)));
                }
            }
            result.add(subList);
        }
        return result;
    }
}
