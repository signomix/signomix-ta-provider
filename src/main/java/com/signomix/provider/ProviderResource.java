package com.signomix.provider;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.cedarsoftware.util.io.JsonWriter;
import com.signomix.common.DateTool;
import com.signomix.common.Token;
import com.signomix.common.iot.ChannelData;

import io.quarkus.runtime.StartupEvent;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.OPTIONS;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

@Path("/api")
@ApplicationScoped
public class ProviderResource {
    private static final Logger LOG = Logger.getLogger(ProviderResource.class);
    private AtomicLong tracing = new AtomicLong();

    @Inject
    EventBus bus;

    @Inject
    ProviderService service;

    @ConfigProperty(name = "device.authorization.required")
    Boolean authorizationRequired;

    @ConfigProperty(name = "device.eui.header.first")
    Boolean euiHeaderFirst;

    HashMap<String, Object> args;
    boolean prettyPrint = true;

    public void onApplicationStart(@Observes StartupEvent event) {
        args = new HashMap<>();
        args.put(JsonWriter.PRETTY_PRINT, prettyPrint);
        args.put(JsonWriter.DATE_FORMAT, "dd/MMM/yyyy:kk:mm:ss Z");
        args.put(JsonWriter.TYPE, false);
    }

    @Path("/provider")
    @OPTIONS
    public String sendOKString() {
        return "OK";
    }

    @Path("/provider/v2/device/{device}/{channel}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDeviceDataVer2(
            @HeaderParam("Authentication") String sessionToken,
            @PathParam("device") String deviceEUI,
            @PathParam("channel") String channelName,
            @QueryParam("query") String query) {
        long trackingId = tracing.incrementAndGet();
        String result;
        String userID = null;
        long t0 = System.currentTimeMillis();
        if (authorizationRequired) {
            Token token = service.getSessionToken(sessionToken);
            userID = token.getUid();
            if (null == userID) {
                return Response.status(Status.UNAUTHORIZED).entity("not authorized").build();
            }
        }
        long t1 = System.currentTimeMillis();
        LOG.debug("trackingID:" + trackingId + " authorization [ms]: " + (t1 - t0));
        List list = service.getDataVer2(userID, deviceEUI, channelName, query);
        long t2 = System.currentTimeMillis();
        LOG.debug("trackingID:" + trackingId + " query [ms]: " + (t2 - t1) + query);
        result = format(list, "json");
        long t3 = System.currentTimeMillis();
        LOG.debug("trackingID:" + trackingId + " formatting [ms]: " + (t3 - t2));
        LOG.debug("trackingID:" + trackingId + " total [ms]: " + (t3 - t0));
        return Response.ok(result).build();
    }

    @Path("/provider/v2/device/{device}/{channel}")
    @GET
    @Produces("text/csv")
    public Response getDeviceDataVer2Csv(
            @HeaderParam("Authentication") String sessionToken,
            @PathParam("device") String deviceEUI,
            @PathParam("channel") String channelName,
            @QueryParam("query") String query,
            @QueryParam("zone") String zone) {
        long trackingId = tracing.incrementAndGet();
        String result;
        String userID = null;
        long t0 = System.currentTimeMillis();
        if (authorizationRequired) {
            userID = service.getSessionToken(sessionToken).getUid();
            if (null == userID) {
                return Response.status(Status.UNAUTHORIZED).entity("not authorized").build();
            }
        }
        long t1 = System.currentTimeMillis();
        LOG.debug("trackingID:" + trackingId + " authorization [ms]: " + (t1 - t0));
        List list = service.getDataVer2(userID, deviceEUI, channelName, query);
        long t2 = System.currentTimeMillis();
        LOG.debug("trackingID:" + trackingId + " query [ms]: " + (t2 - t1) + query);
        result = formatVer2(list, "csv", zone);
        long t3 = System.currentTimeMillis();
        LOG.debug("trackingID:" + trackingId + " formatting [ms]: " + (t3 - t2));
        LOG.debug("trackingID:" + trackingId + " total [ms]: " + (t3 - t0));
        return Response.ok(result).build();
    }

    @Path("/provider/device/{device}/{channel}")
    @GET
    @Produces("text/csv")
    public Response getDeviceData3(
            @PathParam("device") String deviceEUI,
            @PathParam("channel") String channelName,
            @QueryParam("tid") String sessionToken,
            @QueryParam("query") String query) {
        // List result;
        String result;
        String userID = null;
        long t0 = System.currentTimeMillis();
        if (authorizationRequired) {
            userID = service.getSessionToken(sessionToken).getUid();
            if (null == userID) {
                return Response.status(Status.UNAUTHORIZED).entity("not authorized").build();
            }
        }
        long t1 = System.currentTimeMillis();
        LOG.debug("Authorization time [ms]: " + (t1 - t0));
        List list = service.getData(userID, deviceEUI, channelName, query);
        long t2 = System.currentTimeMillis();
        LOG.debug("DB query time [ms]: " + (t2 - t1) + query);
        result = format(list, "csv");
        LOG.debug("Format time [ms]: " + (System.currentTimeMillis() - t2));
        return Response.ok(result).build();
    }

    @Path("/provider/group/{group}/{channel}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getGroupData2(
            @PathParam("group") String groupEUI,
            @PathParam("channel") String channelNames,
            @QueryParam("tid") String sessionToken,
            @QueryParam("query") String query) {
        // List result;
        String result;
        // String userID = null;
        Token token = null;
        if (authorizationRequired) {
            token = service.getSessionToken(sessionToken);
            // userID = service.getUserID(sessionToken);
            if (null == token) {
                return Response.status(Status.UNAUTHORIZED).entity("not authorized").build();
            }
        }
        // if (query == null || query.isEmpty() || query.equals("undefined")) {
        result = format(service.getGroupLastData(token, groupEUI, channelNames, query), "json");
        // } else {
        // TODO: is it needed?
        // result = format(service.getGroupData(token, groupEUI, channelNames, query),
        // "json");
        // }
        // result = format(service.getGroupData(userID, groupEUI, channelName, query));

        return Response.ok(result).build();
    }

    public String format(Object o, String type) {
        if (null == o) {
            return null;
        }
        String result = "";
        if ("csv".equals(type)) {
            return toCsv((List<List>) o, ",", "\r\n");
        } else {
            result = JsonWriter.objectToJson(o, args);
        }
        return result;
    }

    public String formatVer2(Object o, String type, String zoneId) {
        if (null == o) {
            return null;
        }
        String result = "";
        if ("csv".equals(type)) {
            return toCsvVer2((List<List>) o, ",", "\r\n", zoneId);
        } else {
            result = JsonWriter.objectToJson(o, args);
        }
        return result;
    }

    private String toCsv(List<List> input, String fieldSeparator, String lineSeparator) {
        StringBuffer sb = new StringBuffer();
        List<ChannelData> sublist;
        boolean headerLinePresent = false;
        ChannelData cData;
        if (input.size() < 1) {
            return "";
        }
        List<List> data = input.get(0);
        Double value;
        for (int i = 0; i < data.size(); i++) {
            sublist = (List<ChannelData>) data.get(i);
            if (!headerLinePresent) {
                sb.append("timestamp");
                for (int j = 0; j < sublist.size(); j++) {
                    cData = sublist.get(j);
                    sb.append(fieldSeparator);
                    value = cData.getValue();
                    if(value != null){
                        sb.append(value);
                    }
                }
                sb.append(lineSeparator);
                headerLinePresent = true;
            }
            cData = sublist.get(0);
            sb.append(cData.getTimestamp());
            for (int j = 0; j < sublist.size(); j++) {
                cData = sublist.get(j);
                sb.append(",").append(cData.getValue());
            }
            sb.append(lineSeparator);
        }
        return sb.toString();
    }

    private String toCsvVer2(List<List> input, String fieldSeparator, String lineSeparator, String zoneId) {
        StringBuffer sb = new StringBuffer();
        ChannelData channelData;
        if (input.size() < 1) {
            return "";
        }
        // header line
        sb.append("timestamp");
        List<ChannelData> dataFromTimestamp = (List<ChannelData>)input.get(0);
        for (int i = 0; i < dataFromTimestamp.size(); i++) {
            channelData = dataFromTimestamp.get(i);
            sb.append(fieldSeparator).append(channelData.getName());
        }
        sb.append(lineSeparator);
        // data lines
        Double value;
        for (int i = 0; i < input.size(); i++) {
            List<ChannelData> data = (List<ChannelData>)input.get(i);
            sb.append(DateTool.getTimestampAsIsoInstant(data.get(0).getTimestamp(),zoneId));
            for (int j = 0; j < data.size(); j++) {
                channelData = data.get(j);
                sb.append(",");
                value = channelData.getValue();
                if(value != null){
                    sb.append(value);
                }
            }
            sb.append(lineSeparator);
        }
        return sb.toString();
}

}