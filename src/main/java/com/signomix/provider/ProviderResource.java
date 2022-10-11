package com.signomix.provider;

import java.util.HashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
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

import io.quarkus.runtime.StartupEvent;
import io.vertx.mutiny.core.eventbus.EventBus;

@Path("/api")
@ApplicationScoped
public class ProviderResource {
    private static final Logger LOG = Logger.getLogger(ProviderResource.class);

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

    @Path("/provider/device/{device}/{channel}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDeviceData2(
            @PathParam("device") String deviceEUI,
            @PathParam("channel") String channelName,
            @QueryParam("tid") String sessionToken,
            @QueryParam("query") String query) {
        // List result;
        String result;
        String userID = null;
        if (authorizationRequired) {
            userID = service.getUserID(sessionToken);
            if (null == userID) {
                return Response.status(Status.UNAUTHORIZED).entity("not authorized").build();
            }
        }
        result = format(service.getData(userID, deviceEUI, channelName, query));
        return Response.ok(result).build();
    }

    @Path("/provider/group/{group}/{channel}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getGroupData2(
            @PathParam("group") String groupEUI,
            @PathParam("channel") String channelName,
            @QueryParam("tid") String sessionToken,
            @QueryParam("query") String query) {
        // List result;
        String result;
        String userID = null;
        if (authorizationRequired) {
            userID = service.getUserID(sessionToken);
            if (null == userID) {
                return Response.status(Status.UNAUTHORIZED).entity("not authorized").build();
            }
        }
        result = format(service.getData(userID, groupEUI, channelName, query));
        return Response.ok(result).build();
    }

    public String format(Object o) {
        if(null==o){
            return null;
        }
        return JsonWriter.objectToJson(o, args);
    }

}