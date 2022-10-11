package com.signomix.common.db;

import io.agroal.api.AgroalDataSource;

public interface AuthDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public String getUser(String token);
}
