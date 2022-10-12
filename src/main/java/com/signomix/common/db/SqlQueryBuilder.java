package com.signomix.common.db;

import org.jboss.logging.Logger;

public class SqlQueryBuilder {
    private static final Logger LOG = Logger.getLogger(SqlQueryBuilder.class);

    

    public static String buildSqlQueryVirtual(DataQuery dq) {
        String query = "SELECT eui,tstamp,data FROM virtualdevicedata WHERE eui=?";
        return query;
    }

    public static String buildDeviceDataQuery(int channelIndex, DataQuery dq) {
        if (dq.isVirtual()) {
            return buildSqlQueryVirtual(dq);
        }
        String query;
        String defaultQuery;
        if (channelIndex >= 0) {
            String columnName = "d" + (channelIndex);
            defaultQuery = "select eui,userid,day,dtime,tstamp," + columnName + ","
                    + "project,state from devicedata where eui=?";
        } else {
            defaultQuery = "select eui,userid,day,dtime,tstamp,"
                    + "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24"
                    + " from devicedata where eui=?";
        }
        String projectQuery = " and project=?";
        String statusQuery = " and state=?";
        String wherePart = " and tstamp between ? and ?";
        String orderPart = " order by tstamp desc limit ?";
        query = defaultQuery;
        if (null != dq.getProject()) {
            query = query.concat(projectQuery);
        }
        if (null != dq.getState()) {
            query = query.concat(statusQuery);
        }
        LOG.debug("fromTs:"+dq.getFromTs());
        LOG.debug("toTs:"+dq.getToTs());
        if (null != dq.getFromTs() && null != dq.getToTs()) {
            query = query.concat(wherePart);
        }
        query = query.concat(orderPart);
        System.out.println(query);
        return query;
    }



}
