package com.signomix.provider.adapter.out;

import com.signomix.common.db.ReportResult;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ReportsService {
    
    /* @Inject
    ReportsClient client; */

    public ReportResult getReport(String authentication, String query) {
        // return client.getReport(authentication, query);
        SimpleReportsClient simpleReportsClient = new SimpleReportsClient();
        String report = simpleReportsClient.getReport("sgx_fc8a0f10069a8381b48b7b00282f3300", query);
        return ReportResult.parse(report);
    }

}
