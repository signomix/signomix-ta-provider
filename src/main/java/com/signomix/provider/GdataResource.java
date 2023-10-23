package com.signomix.provider;

import java.util.HashMap;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.cedarsoftware.util.io.JsonWriter;
import com.signomix.common.Token;
import com.signomix.common.iot.ChannelData;
import com.signomix.provider.formatter.kanarek.KanarekFormatter;

import io.quarkus.runtime.StartupEvent;

@Path("/api/iot/gdata")
@ApplicationScoped
public class GdataResource {
    private static final Logger LOG = Logger.getLogger(GdataResource.class);

    @Inject
    ProviderService service;

    @ConfigProperty(name = "device.authorization.required")
    Boolean authorizationRequired;

    @ConfigProperty(name = "device.eui.header.first")
    Boolean euiHeaderFirst;

    HashMap<String, Object> args;
    boolean prettyPrint = true;

    @Inject
    KanarekFormatter kanarekFormatter;

    public void onApplicationStart(@Observes StartupEvent event) {
        args = new HashMap<>();
        args.put(JsonWriter.PRETTY_PRINT, prettyPrint);
        args.put(JsonWriter.DATE_FORMAT, "dd/MMM/yyyy:kk:mm:ss Z");
        args.put(JsonWriter.TYPE, false);
    }

    @Path("/{path}")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getDeviceData2(
            @PathParam("path") String path,
            @QueryParam("tid") String sessionTokenID,
            @QueryParam("format") String format) {

        Token token = null;
        if (authorizationRequired) {
            token = service.getSessionToken(sessionTokenID);
            if (null == token) {
                return Response.status(Status.UNAUTHORIZED).entity("not authorized").build();
            }
        }
        String channels = null;
        String groupID;
        if (path == null || path.isEmpty()) {
            return Response.status(Status.BAD_REQUEST).entity("bad request").build();
        }
        String[] params = path.split("/");
        if (params.length == 0) {
            return Response.status(Status.BAD_REQUEST).entity("bad request").build();
        }
        groupID = params[0];
        if (params.length > 1) {
            channels = params[1];
        }

        List result = service.getGroupLastData(token, groupID, channels);

        if (null == format || format.isEmpty()) {
            format = "json";
        }
        String response = format(result, format);
        return Response.ok(response).build();
    }

    private String format(List list, String format) {
        String result = null;
        if (format.equals("csv")) {
            result = formatCSV(list);
        } else if (format.equals("kanarek")) {
            result = kanarekFormatter.format(prettyPrint, list);
        } else {
            result = JsonWriter.objectToJson(list, args);
        }
        return result;
    }

    private String formatCSV(List list) {
        StringBuilder sb = new StringBuilder();
        sb.append("EUI");
        sb.append(",");
        sb.append("CHANNEL");
        sb.append(",");
        sb.append("VALUE");
        sb.append(",");
        sb.append("TIMESTAMP");
        sb.append("\n");
        for (Object o : list) {
            ChannelData cd = (ChannelData) o;
            sb.append(cd.getDeviceEUI());
            sb.append(",");
            sb.append(cd.getName());
            sb.append(",");
            sb.append(cd.getValue());
            sb.append(",");
            sb.append(cd.getTimestamp());
            sb.append("\n");
        }
        return sb.toString();
    }

}