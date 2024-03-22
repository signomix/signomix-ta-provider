package com.signomix.provider;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.signomix.common.Token;
import com.signomix.common.db.AuthDaoIface;
import com.signomix.common.db.DataQuery;
import com.signomix.common.db.DataQueryException;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.DeviceGroup;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.cache.CacheResult;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class ProviderService {
    private static final Logger LOG = Logger.getLogger(ProviderService.class);

    // TODO: test /q/health/ready

    // TODO: move to config
    private long sessionTokenLifetime = 30; // minutes
    private long permanentTokenLifetime = 10 * 365 * 24 * 60; // 10 years in minutes

    @Inject
    @DataSource("iot")
    AgroalDataSource ds;

    @Inject
    @DataSource("auth")
    AgroalDataSource ds2;

    @Inject
    @DataSource("oltp")
    AgroalDataSource tsDs;

    IotDatabaseIface dataDao = null;
    AuthDaoIface authDao = null;

    @ConfigProperty(name = "signomix.app.key", defaultValue = "not_configured")
    String appKey;
/*     @ConfigProperty(name = "signomix.core.host", defaultValue = "not_configured")
    String coreHost;
 */    @ConfigProperty(name = "signomix.query.limit", defaultValue = "500")
    String requestLimitConfigured;

    @ConfigProperty(name = "signomix.database.type")
    String databaseType;

    public void onApplicationStart(@Observes StartupEvent event) {
        if ("postgresql".equalsIgnoreCase(databaseType)) {
            LOG.info("using postgresql database");
            dataDao = new com.signomix.common.tsdb.IotDatabaseDao();
            dataDao.setDatasource(tsDs);
            authDao = new com.signomix.common.tsdb.AuthDao();
            authDao.setDatasource(tsDs);
        } else if ("h2".equalsIgnoreCase(databaseType)) {
            LOG.info("using mysql database");
            dataDao = new com.signomix.common.db.IotDatabaseDao();
            dataDao.setDatasource(ds);
            authDao = new com.signomix.common.db.AuthDao();
            authDao.setDatasource(ds2);
        } else {
            LOG.error("database type not configured");
        }

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
        return authDao.getUserId(sessionToken, sessionTokenLifetime, permanentTokenLifetime);
    }

    @CacheResult(cacheName = "issuer-cache")
    String getIssuerID(String sessionToken) {
        LOG.debug("token:" + sessionToken);
        return authDao.getUserId(sessionToken, sessionTokenLifetime, permanentTokenLifetime);
    }

    Token getSessionToken(String sessionToken) {
        LOG.info("token:" + sessionToken);
        return authDao.getToken(sessionToken, sessionTokenLifetime, permanentTokenLifetime);
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

    @CacheResult(cacheName = "query-cache")
    List getDataVer2(String userID, String deviceEUI, String channelName, String query) {
        LOG.debug("userID:" + userID);
        LOG.debug("device:" + deviceEUI);
        LOG.debug("channel:" + channelName);
        LOG.debug("query:" + query);
        try {
            if (null == channelName || channelName.isEmpty()) {
                return dataDao.getValues2(userID, deviceEUI, query);
            } else {
                return dataDao.getValues2(userID, deviceEUI, query + " channel " + channelName);
            }
        } catch (IotDatabaseException ex) {
            ex.printStackTrace();
            LOG.warn(ex.getMessage());
            return new ArrayList();
        }

    }

    @CacheResult(cacheName = "group-query-cache")
    List getGroupLastData(Token token, String groupEUI, String channelNames, String query) {
        long secondsBack = 3600;
        if (null != query && !query.isEmpty()) {
            DataQuery dq;
            //LOG.info("query:" + query);
            try {
                dq = DataQuery.parse(query);
                if (dq.getOffset() != 0) {
                    secondsBack = dq.getOffset() / 1000; // offset is in ms
                }
                //LOG.info("secondsBack:" + secondsBack);
            } catch (DataQueryException ex) {
                LOG.warn(ex.getMessage());
            }
        }
        //LOG.debug("secondsBack:" + secondsBack);
        long organizationId = -1;
        // LOG.debug("query:" + query);
        DeviceGroup group = null;
        try {
            group = dataDao.getGroup(groupEUI);
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (null == group || !hasAccessRights(token.getUid(), group)) {
            LOG.warn("no access rights");
            return null;
        }
        String[] channels;
        if (channelNames == null || channelNames.isEmpty()) {
            HashMap<String, Object> map = group.getChannels();
            channels = map.keySet().toArray(new String[map.size()]);
        } else {
            channels = channelNames.split(",");
        }
        try {
            return dataDao.getGroupLastValues(token.getUid(), organizationId, groupEUI, channels, secondsBack);
        } catch (IotDatabaseException ex) {
            LOG.warn(ex.getMessage());
            return new ArrayList();
        }

    }

    private boolean hasAccessRights(String userID, DeviceGroup group) {
        if (null == group) {
            return false;
        }
        String[] admins = group.getAdministrators().split(",");
        String[] team = group.getTeam().split(",");
        if (group.getUserID().equals(userID)) {
            return true;
        }
        if (admins.length > 0) {
            for (int i = 0; i < admins.length; i++) {
                if (admins[i].equals(userID)) {
                    return true;
                }
            }
        }
        if (team.length > 0) {
            for (int i = 0; i < team.length; i++) {
                if (team[i].equals(userID)) {
                    return true;
                }
            }
        }
        return false;
    }

    List<String> getGroupChannelNames(String groupEUI, String channelNames, String query){
        String[] channels = channelNames.split(",");
        if(channels.length==1 && "*".equals(channels[0])){
            ArrayList<String> result = new ArrayList<>();
            try {
                dataDao.getGroup(groupEUI).getChannels().keySet().forEach((k)->result.add((String)k));
            } catch (IotDatabaseException e) {
                LOG.error(e.getMessage());
            }
            return result;
        }else{
            return Arrays.asList(channels);
        }
        
    }

    // @CacheResult(cacheName = "group-query-cache")
    List getGroupData(Token token, String groupEUI, String channelNames, String query) {
        LOG.info("group:" + groupEUI);
        LOG.info("channel:" + channelNames);

        long organizationId = -1;
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
            String[] channels = channelNames.split(",");
            return dataDao.getGroupValues(token.getUid(), organizationId, groupEUI, channels, query);
            // return dataDao.getValuesOfGroup(userID, organizationId, groupEUI, channels,
            // secondsBack);
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
        long t0 = System.currentTimeMillis();
        long t1;
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
            t1 = System.currentTimeMillis();
            LOG.debug("normalize time 1:" + (t1 - t0) + " ms");
            return data;
        }
        SortedMap<Long, ChannelData> subMap;
        for (int i = 0; i < numberOfLists; i++) {
            subList = (ArrayList<ChannelData>) data.get(i);
            for (int j = 0; j < subList.size(); j++) {
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
                SortedMap<Long, ChannelData> tmpMap3 = map.get(channelNames.get(j));
                if (null != tmpMap3) {
                    subList.add(tmpMap3.get(timestamps.get(i)));
                }
            }
            result.add(subList);
        }
        t1 = System.currentTimeMillis();
        LOG.debug("normalize time 2:" + (t1 - t0) + " ms");
        return result;
    }
}
