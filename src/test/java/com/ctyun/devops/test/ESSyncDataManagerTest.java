package com.ctyun.devops.test;

import com.ctyun.devops.service.ESSyncDataManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ESSyncDataManagerTest {

    @Autowired
    private ESSyncDataManager esSyncDataManager;

    @Test
    public void testSyncAllData() {
        esSyncDataManager.syncAllData();
    }
}
