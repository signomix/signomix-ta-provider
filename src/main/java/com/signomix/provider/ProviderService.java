package com.signomix.provider;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.signomix.common.db.AuthDao;
import com.signomix.common.db.AuthDaoIface;
import com.signomix.common.db.IotDatabaseDao;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;

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
        authDao=new AuthDao();
        authDao.setDatasource(ds2);
        try{
            LOG.info("requestLimitConfigured:"+requestLimitConfigured);
            int requestLimit=Integer.parseInt(requestLimitConfigured);
            dataDao.setQueryResultsLimit(requestLimit);
        }catch(Exception e){
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
                return result;
            } else {
                return (ArrayList) dataDao.getValues(userID, deviceEUI, query);
            }
        } catch (IotDatabaseException ex) {
            LOG.warn(ex.getMessage());
            return new ArrayList();
        }

    }

    List getGroupData(String userID, String groupEUI, String channelNames) {
        LOG.debug("group:" + groupEUI);
        LOG.debug("channel:" + channelNames);

        long organizationId=-1;
        long secondsBack=3600;
        //LOG.debug("query:" + query);
        try {
            //if (channelName != null && !"$".equals(channelName)) {
            //    String query2=null!=query?query:"";
            //    query2=query2+" channel " + channelName;
            //    ArrayList result = (ArrayList) dataDao.getGroupValues(userID, groupEUI,query2);
            //    return new ArrayList<>();
            //} else {
                //return (ArrayList) dataDao.getGroupValues(userID, groupEUI, channelNames);
                return dataDao.getValuesOfGroup(userID, organizationId, groupEUI, channelNames, secondsBack);
            //}
        } catch (IotDatabaseException ex) {
            LOG.warn(ex.getMessage());
            return new ArrayList();
        }

    }
}
