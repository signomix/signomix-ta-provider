package com.signomix.provider.adapter.out;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import com.signomix.common.db.ReportResult;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

@RegisterRestClient(configKey="reports-api")
@Path("/api/reports")
public interface ReportsClient {

    @GET
    @Path("/single")
    public ReportResult getReport(
        @HeaderParam("Authentication") String authentication, 
        @QueryParam("query") String query);

}
