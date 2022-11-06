package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.cedarsoftware.util.io.JsonObject;
import com.cedarsoftware.util.io.JsonReader;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.virtual.VirtualData;

import io.agroal.api.AgroalDataSource;

public class IotDatabaseDao implements IotDatabaseIface {
    private static final Logger LOG = Logger.getLogger(IotDatabaseDao.class);

    private AgroalDataSource dataSource;

    @ConfigProperty(name = "signomix.query.limit", defaultValue = "500")
    String requestLimitConfigured;

    Integer requestLimit = 100;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void setQueryResultsLimit(int limit){
        requestLimit=limit;
        LOG.info("requestLimit:"+requestLimit);
    }

    @Override
    public List<List> getValues(String userID, String deviceEUI, String dataQuery)
            throws IotDatabaseException {
        LOG.debug("queryLimit:" + requestLimit);
        DataQuery dq;
        try {
            dq = DataQuery.parse(dataQuery);
        } catch (DataQueryException ex) {
            throw new IotDatabaseException(ex.getCode(), "DataQuery " + ex.getMessage());
        }
        if (dq.isVirtual()) {
            return getVirtualDeviceMeasures(userID, deviceEUI, dq);
        }
        if (null != dq.getGroup()) {
            String channelName = dq.getChannelName();
            if (null == channelName) {
                channelName = "";
            }
            // return getValuesOfGroup(userID, dq.getGroup(), channelName.split(","),
            // defaultGroupInterval, dq);
            return new ArrayList<>();
        }

        int limit = dq.getLimit();
        if (dq.average > 0) {
            limit = dq.average;
        }
        if (dq.minimum > 0) {
            limit = dq.minimum;
        }
        if (dq.maximum > 0) {
            limit = dq.maximum;
        }
        if (dq.summary > 0) {
            limit = dq.summary;
        }
        List<List> result = new ArrayList<>();
        if (dq.getNewValue() != null) {
            limit = limit - 1;
        }

        if (null == dq.getChannelName() || "*".equals(dq.getChannelName())) {
            // TODO
            result.add(getValues(userID, deviceEUI, limit, dq));
            return result;
        }
        boolean singleChannel = !dq.getChannelName().contains(",");
        if (singleChannel) {
            result.add(getChannelValues(userID, deviceEUI, dq.getChannelName(), limit, dq)); // project
        } else {
            String[] channels = dq.getChannelName().split(",");
            List<ChannelData>[] temp = new ArrayList[channels.length];
            for (int i = 0; i < channels.length; i++) {
                temp[i] = getChannelValues(userID, deviceEUI, channels[i], limit, dq); // project
            }
            List<ChannelData> values;
            for (int i = 0; i < limit; i++) {
                values = new ArrayList<>();
                for (int j = 0; j < channels.length; j++) {
                    if (temp[j].size() > i) {
                        values.add(temp[j].get(i));
                    }
                }
                if (values.size() > 0) {
                    result.add(values);
                }
            }
        }
        if (!singleChannel) {
            return result;
        }

        ChannelData data = new ChannelData(dq.getChannelName(), 0.0, System.currentTimeMillis());
        data.setNullValue();
        List<ChannelData> subResult = new ArrayList<>();
        Double actualValue = null;
        Double tmpValue;
        int size = 0;
        if (dq.average > 0) {
            if (result.size() > 0) {
                size = result.get(0).size();
                for (int i = 0; i < size; i++) {
                    if (i == 0) {
                        actualValue = ((ChannelData) result.get(0).get(i)).getValue();
                    } else {
                        actualValue = actualValue + ((ChannelData) result.get(0).get(i)).getValue();
                    }
                }
            }
            if (dq.getNewValue() != null) {
                if (null != actualValue) {
                    actualValue = actualValue + dq.getNewValue();
                } else {
                    actualValue = dq.getNewValue();
                }
                data.setValue(actualValue / (size + 1));
            } else {
                if (size > 0) {
                    data.setValue(actualValue / size);
                }
            }
            subResult.add(data);
            result.clear();
            result.add(subResult);
        } else if (dq.maximum > 0) {
            actualValue = Double.MIN_VALUE;
            if (result.size() > 0) {
                size = result.get(0).size();
                for (int i = 0; i < size; i++) {
                    tmpValue = ((ChannelData) result.get(0).get(i)).getValue();
                    if (tmpValue.compareTo(actualValue) > 0) {
                        actualValue = tmpValue;
                    }
                }
            }
            if (dq.getNewValue() != null && dq.getNewValue() > actualValue) {
                actualValue = dq.getNewValue();
            }
            if (actualValue.compareTo(Double.MIN_VALUE) > 0) {
                data.setValue(actualValue);
            }
            subResult.add(data);
            result.clear();
            result.add(subResult);
        } else if (dq.minimum > 0) {
            actualValue = Double.MAX_VALUE;
            if (result.size() > 0) {
                size = result.get(0).size();
                for (int i = 0; i < size; i++) {
                    tmpValue = ((ChannelData) result.get(0).get(i)).getValue();
                    if (tmpValue.compareTo(actualValue) < 0) {
                        actualValue = tmpValue;
                    }
                }
            }
            if (dq.getNewValue() != null && dq.getNewValue() < actualValue) {
                actualValue = dq.getNewValue();
            }
            if (actualValue.compareTo(Double.MAX_VALUE) < 0) {
                data.setValue(actualValue);
            }
            subResult.add(data);
            result.clear();
            result.add(subResult);
        } else if (dq.summary > 0) {
            actualValue = null;
            if (result.size() > 0) {
                size = result.get(0).size();
                for (int i = 0; i < size; i++) {
                    if (i == 0) {
                        actualValue = ((ChannelData) result.get(0).get(i)).getValue();
                    } else {
                        actualValue = actualValue + ((ChannelData) result.get(0).get(i)).getValue();
                    }
                }
            }
            if (dq.getNewValue() != null) {
                if (null == actualValue) {
                    actualValue = actualValue + dq.getNewValue();
                } else {
                    actualValue = dq.getNewValue();
                }
            }
            if (null != actualValue) {
                data.setValue(actualValue);
            }
            subResult.add(data);
            result.clear();
            result.add(subResult);
        }

        return result;
    }

    private List<List> getValues(String userID, String deviceEUI, int limit, DataQuery dataQuery)
            throws IotDatabaseException {
        String query = SqlQueryBuilder.buildDeviceDataQuery(-1, dataQuery);
        List<String> channels = getDeviceChannels(deviceEUI);
        List<List> result = new ArrayList<>();
        ArrayList<ChannelData> row;
        ArrayList row2;
        // System.out.println("SQL QUERY: " + query);
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            int paramIdx = 2;
            if (null != dataQuery.getProject()) {
                pst.setString(paramIdx, dataQuery.getProject());
                paramIdx++;
                if (null != dataQuery.getState()) {
                    pst.setDouble(paramIdx, dataQuery.getState());
                    paramIdx++;
                }
            } else {
                if (null != dataQuery.getState()) {
                    pst.setDouble(paramIdx, dataQuery.getState());
                    paramIdx++;
                }
            }
            if (null != dataQuery.getFromTs() && null != dataQuery.getToTs()) {
                // System.out.println("fromTS: " + dataQuery.getFromTs().getTime());
                pst.setTimestamp(paramIdx, dataQuery.getFromTs());
                paramIdx++;
                // System.out.println("toTS: " + dataQuery.getToTs().getTime());
                pst.setTimestamp(paramIdx, dataQuery.getToTs());
                paramIdx++;
            }
            pst.setInt(paramIdx, dataQuery.getLimit() == 0 ? limit : dataQuery.getLimit());

            ResultSet rs = pst.executeQuery();
            if (dataQuery.isTimeseries()) {
                row2 = new ArrayList();
                row2.add("timestamp");
                for (int i = 0; i < channels.size(); i++) {
                    row2.add(channels.get(i));
                }
                result.add(row2);
            }
            double d;
            while (rs.next()) {
                if (dataQuery.isTimeseries()) {
                    row2 = new ArrayList();
                    row2.add(rs.getTimestamp(5).getTime());
                    for (int i = 0; i < channels.size(); i++) {
                        d = rs.getDouble(6 + i);
                        if (!rs.wasNull()) {
                            row2.add(d);
                        } else {
                            row2.add(null);
                        }
                    }
                    result.add(row2);
                } else {
                    row = new ArrayList<>();
                    for (int i = 0; i < channels.size(); i++) {
                        d = rs.getDouble(6 + i);
                        if (!rs.wasNull()) {
                            row.add(new ChannelData(deviceEUI, channels.get(i), d,
                                    rs.getTimestamp(5).getTime()));
                        }
                    }
                    result.add(row);
                }
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
    }

    private List<ChannelData> getChannelValues(String userID, String deviceEUI, String channel, int resultsLimit,
            DataQuery dataQuery) throws IotDatabaseException {
        ArrayList<ChannelData> result = new ArrayList<>();
        int channelIndex = getChannelIndex(deviceEUI, channel);
        if (channelIndex < 1) {
            return result;
        }
        String query = SqlQueryBuilder.buildDeviceDataQuery(channelIndex, dataQuery);
        int limit = resultsLimit;
        if (requestLimit > 0 && requestLimit < limit) {
            limit = requestLimit;
        }
        LOG.debug(query);
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);

            int paramIdx = 2;
            if (null != dataQuery.getProject()) {
                pst.setString(paramIdx, dataQuery.getProject());
                paramIdx++;
                if (null != dataQuery.getState()) {
                    pst.setDouble(paramIdx, dataQuery.getState());
                    paramIdx++;
                }
            } else {
                if (null != dataQuery.getState()) {
                    pst.setDouble(paramIdx, dataQuery.getState());
                    paramIdx++;
                }
            }
            if (null != dataQuery.getFromTs() && null != dataQuery.getToTs()) {
                pst.setTimestamp(paramIdx, dataQuery.getFromTs());
                paramIdx++;
                pst.setTimestamp(paramIdx, dataQuery.getToTs());
                paramIdx++;
            }
            pst.setInt(paramIdx, limit);

            ResultSet rs = pst.executeQuery();
            Double d;
            while (rs.next()) {
                d = rs.getDouble(6);
                if (!rs.wasNull()) {
                    result.add(0, new ChannelData(deviceEUI, channel, d, rs.getTimestamp(5).getTime()));
                }
            }
            return result;
        } catch (SQLException e) {
            LOG.error("problematic query = " + query);
            e.printStackTrace();
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
    }

    private List<List> getVirtualDeviceMeasures(String userID, String deviceEUI, DataQuery dataQuery)
            throws IotDatabaseException {
        List<List> result = new ArrayList<>();
        String query = SqlQueryBuilder.buildDeviceDataQuery(-1, dataQuery);
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            String eui;
            Timestamp ts;
            String serializedData;
            ChannelData cData;
            ArrayList<ChannelData> channels = new ArrayList<>();
            String channelName;
            while (rs.next()) {
                eui = rs.getString(1);
                ts = rs.getTimestamp(2);
                serializedData = rs.getString(3);
                JsonObject jo = (JsonObject) JsonReader.jsonToJava(serializedData);
                VirtualData vd = new VirtualData(eui);
                vd.timestamp = ts.getTime();
                JsonObject fields = (JsonObject) jo.get("payload_fields");
                Iterator<String> it = fields.keySet().iterator();
                while (it.hasNext()) {
                    channelName = it.next();
                    cData = new ChannelData();
                    cData.setDeviceEUI(eui);
                    cData.setTimestamp(vd.timestamp);
                    cData.setName(channelName);
                    cData.setValue((Double) fields.get(channelName));
                    channels.add(cData);
                }
            }
            result.add(channels);
        } catch (SQLException e) {
            LOG.error("problematic query = " + query);
            e.printStackTrace();
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return result;
    }

    private int getChannelIndex(String deviceEUI, String channel) throws IotDatabaseException {
        return getDeviceChannels(deviceEUI).indexOf(channel) + 1;
    }

    @Override
    public List<List<List>> getValuesOfGroup(String userID, long organizationId, String groupEUI, String channelNames, long secondsBack)
            throws IotDatabaseException {
                String[] channels=channelNames.split(",");
        return getGroupLastValues(userID, organizationId, groupEUI, channels, secondsBack);
    }

    @Override
    public List<String> getDeviceChannels(String deviceEUI) throws IotDatabaseException {
        List<String> channels;
        String query = "select channels from devicechannels where eui=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                String[] s = rs.getString(1).toLowerCase().split(",");
                channels = Arrays.asList(s);
                String channelStr = "";
                for (int i = 0; i < channels.size(); i++) {
                    channelStr = channelStr + channels.get(i) + ",";
                }
                LOG.debug("CHANNELS READ: " + deviceEUI + " " + channelStr);
                return channels;
            } else {
                return new ArrayList<>();
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    public List<String> getGroupChannels(String groupEUI) throws IotDatabaseException {
        List<String> channels;
        // return ((Service) Kernel.getInstance()).getDataStorageAdapter().
        String query = "select channels from groups where eui=?";
        channels = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, groupEUI);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                String[] s = rs.getString(1).toLowerCase().split(",");
                channels = Arrays.asList(s);
            }
            return channels;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    /**
     * Get last registered measuresfrom groupDevices dedicated to the specified
     * group
     *
     * @param userID
     * @param groupEUI
     * @param channelNames
     * @return
     * @throws ThingsDataException
     */
    /* 
    public List<List<List>> getValuesOfGroup(String userID, long organizationID, String groupEUI, String[] channelNames,
            long interval,
            String dataQuery)
            throws IotDatabaseException {
        DataQuery dq;
        if (dataQuery.isEmpty()) {
            dq = new DataQuery();
            dq.setFromTs("-" + interval + "s");
        } else {
            try {
                dq = DataQuery.parse(dataQuery);
            } catch (DataQueryException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return new ArrayList<>();
            }
        }
        // return getGroupLastValues(userID, groupEUI, channelNames, interval);
        return getGroupLastValues(userID, organizationID, groupEUI, channelNames, dq);
    }

    public List<List<List>> getValuesOfGroup(String userID, long organizationID, String groupEUI, String[] channelNames,
            long interval,
            DataQuery dataQuery)
            throws IotDatabaseException {
        // return getGroupLastValues(userID, groupEUI, channelNames, interval);
        return getGroupLastValues(userID, organizationID, groupEUI, channelNames, dataQuery);
    }

    public List<List<List>> getGroupLastValues(String userID, long organizationID, String groupEUI,
            String[] channelNames, DataQuery dQuery)
            throws IotDatabaseException {
        List<String> requestChannels = null;
        if (null == dQuery.getChannelName() || dQuery.getChannelName().isEmpty()) {
            String cNames = "";
            for (int i = 0; i < channelNames.length; i++) {
                cNames = cNames.concat(channelNames[i]);
                if (i < channelNames.length - 1) {
                    cNames = cNames.concat(",");
                }
            }
            dQuery.setChannelName(cNames);
        }

        requestChannels = Arrays.asList(channelNames);
        if (null != dQuery.getChannelName()) {
            requestChannels = dQuery.getChannels();
        }
        String group = "%," + groupEUI + ",%";
        String deviceQuery = "SELECT eui,channels FROM devices WHERE groups like ?;";
        ArrayList<String> devices = new ArrayList<>();
        ArrayList<List> channels = new ArrayList<>();
        // String query = buildGroupDataQuery(dQuery);
        // logger.info("query with DataQuery: " + query);
        List<String> groupChannels = getGroupChannels(groupEUI);
        if (requestChannels.size() == 0) {
            //logger.error("empty channelNames");
            requestChannels = groupChannels;
        }
        List<List<List>> result = new ArrayList<>();
        List<List> measuresForEui = new ArrayList<>();
        List<ChannelData> measuresForEuiTimestamp = new ArrayList<>();
        List<ChannelData> tmpResult = new ArrayList<>();
        ChannelData cd;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstd = conn.prepareStatement(deviceQuery);) {
            pstd.setString(1, group);
            ResultSet rs = pstd.executeQuery();
            while (rs.next()) {
                devices.add(rs.getString(1));
                channels.add(Arrays.asList(rs.getString(2).split(",")));
            }
            for (int k = 0; k < devices.size(); k++) {
                result.add(getValues(userID, devices.get(k), 1, dQuery));
            }
            return result;
        } catch (SQLException e) {
            //logger.error(e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception ex) {
            //olgger.error(ex.getMessage());
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, ex.getMessage());
        }
    }
    */

    public List<List<List>> getGroupLastValues(String userID, long organizationID, String groupEUI,
            String[] channelNames, long secondsBack)
            throws IotDatabaseException {
        List<String> requestChannels = Arrays.asList(channelNames);
        try {
            String group = "%," + groupEUI + ",%";
            long timestamp = System.currentTimeMillis() - secondsBack*1000;
            String deviceQuery = "SELECT eui,channels FROM devices WHERE groups like ?;";
            HashMap<String, List> devices = new HashMap<>();
            String query;
            query = "SELECT "
                    + "eui,userid,day,dtime,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24 "
                    + "FROM devicedata "
                    + "WHERE eui IN "
                    + "(SELECT eui FROM devices WHERE groups like ?) "
                    + "and (tstamp>?) "
                    + "order by eui,tstamp desc;";
            List<String> groupChannels = getGroupChannels(groupEUI);
            if (requestChannels.size() == 0) {
                //logger.error("empty channelNames");
                requestChannels = groupChannels;
            }
            List<List<List>> result = new ArrayList<>();
            List<List> measuresForEui = new ArrayList<>();
            List<ChannelData> measuresForEuiTimestamp = new ArrayList<>();
            List<ChannelData> tmpResult = new ArrayList<>();
            ChannelData cd;
            //logger.debug("{} {} {} {} {}", groupEUI, group, groupChannels.size(), requestChannels.size(), query);
            //.info("query withseconds back: " + query);
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstd = conn.prepareStatement(deviceQuery);
                    PreparedStatement pst = conn.prepareStatement(query);) {
                pstd.setString(1, group);
                ResultSet rs = pstd.executeQuery();
                while (rs.next()) {
                    devices.put(rs.getString(1), Arrays.asList(rs.getString(2).split(",")));
                }
                pst.setString(1, group);
                pst.setTimestamp(2, new Timestamp(timestamp));
                rs = pst.executeQuery();
                int channelIndex;
                String channelName;
                String devEui;
                double d;
                while (rs.next()) {
                    for (int i = 0; i < groupChannels.size(); i++) {
                        devEui = rs.getString(1);
                        channelName = groupChannels.get(i);
                        channelIndex = devices.get(devEui).indexOf(channelName);
                        d = rs.getDouble(6 + channelIndex);
                        if (!rs.wasNull()) {
                            tmpResult.add(new ChannelData(devEui, channelName, d,
                                    rs.getTimestamp(5).getTime()));
                        }
                    }
                }
            } catch (SQLException e) {
                //logger.error(e.getMessage());
                throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
            } catch (Exception ex) {
                //logger.error(ex.getMessage());
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, ex.getMessage());
            }
            if (tmpResult.isEmpty()) {
                return result;
            }
            long processedTimestamp = 0;
            String prevEUI = "";
            long prevTimestamp = 0;
            int idx;
            for (int i = 0; i < tmpResult.size(); i++) {
                cd = tmpResult.get(i);
                // logger.info("ChannelData: {} {} {}", cd.getDeviceEUI(), cd.getName(),
                // cd.getTimestamp());
                if (!cd.getDeviceEUI().equalsIgnoreCase(prevEUI)) {
                    if (!measuresForEuiTimestamp.isEmpty()) {
                        measuresForEui.add(measuresForEuiTimestamp);
                    }
                    if (!measuresForEui.isEmpty()) {
                        result.add(measuresForEui);
                    }
                    measuresForEui = new ArrayList<>();
                    measuresForEuiTimestamp = new ArrayList<>();
                    for (int j = 0; j < requestChannels.size(); j++) {
                        measuresForEuiTimestamp.add(null);
                    }
                    idx = requestChannels.indexOf(cd.getName());
                    if (idx > -1) {
                        measuresForEuiTimestamp.set(idx, cd);
                    }
                    prevEUI = cd.getDeviceEUI();
                    prevTimestamp = cd.getTimestamp();
                } else {
                    if (prevTimestamp == cd.getTimestamp()) {
                        // next measurement
                        idx = requestChannels.indexOf(cd.getName());
                        if (idx > -1) {
                            measuresForEuiTimestamp.set(idx, cd);
                        }
                    } else {
                        // skip prevous measures
                    }
                }
            }
            if (!measuresForEuiTimestamp.isEmpty()) {
                measuresForEui.add(measuresForEuiTimestamp);
            }
            if (!measuresForEui.isEmpty()) {
                result.add(measuresForEui);
            }
            return result;
        } catch (Exception e) {
            StackTraceElement[] ste = e.getStackTrace();
            //logger.error("requestChannels[{}]", requestChannels.size());
            //logger.error("channelNames[{}]", channelNames.length);
            //logger.error(e.getMessage());
            for (int i = 0; i < ste.length; i++) {
                //logger.error("{}.{}:{}", e.getStackTrace()[i].getClassName(), e.getStackTrace()[i].getMethodName(),
                //        e.getStackTrace()[i].getLineNumber());
            }
            return null;
        }
    }

}
