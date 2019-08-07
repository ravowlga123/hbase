package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestResultStatsUtil {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestResultStatsUtil.class);

    private static final ServerStatisticTracker serverStatisticTracker = new ServerStatisticTracker();
    private static final ServerName serverName = ServerName.parseServerName("asf903.gq1.ygridcore.net,52690,1517835491385");
    private static final byte[] regionName = {80, 65, 75, 85, 95};
    private static final Result result = new Result();
    private static final RegionLoadStats stats = new RegionLoadStats(100,10,20);
    private static final byte[] null_regionName = null;
    private static final RegionLoadStats null_stats = null;
    private static final StatisticTrackable null_tracker = null;
    private static final RegionInfo regionInfo = RegionInfo.createMobRegionInfo(TableName.valueOf("TestTable"));
    private static final HRegionLocation regionLocation = new HRegionLocation(regionInfo, serverName);
    private static final HRegionLocation null_regionLocation = null;

    @Mock
    StatisticTrackable tracker = mock(StatisticTrackable.class);

    /**
     * Testing public static <T> T updateStats (T r, ServerStatisticTracker serverStats,
     *       ServerName server, byte[] regionName)
     */
    @Test
    public void testUpdateStatsExpectNoResultObject() {

//      Returned object should Not be instance of Result
        ServerStatisticTracker returned_object = ResultStatsUtil.updateStats(serverStatisticTracker, serverStatisticTracker, serverName, regionName);
        Assert.assertNotNull(returned_object);
        Assert.assertEquals(returned_object, serverStatisticTracker);
    }

    @Test
    public void testUpdateStatsExpectResultObject(){
//      Result object should be returned when RegionLoadStats not null
        result.setStatistics(stats);
        Result returned_object = ResultStatsUtil.updateStats(result, serverStatisticTracker, serverName, regionName);
        Assert.assertNotNull(returned_object);
        Assert.assertEquals(returned_object, result);
    }


    @Test
    public void testUpdateStatsRegionLoadStatsNullExpectResult(){
//      Result object should be returned when RegionLoadStats is null
        result.setStatistics(null);
        Result returned_result = ResultStatsUtil.updateStats(result, serverStatisticTracker, serverName, regionName);
        Assert.assertNotNull(returned_result);
        Assert.assertEquals(returned_result, result);
    }

    /**
     * Testing UpdateStatus with signature
     * void UpdateStats(StatisticTrackable tracker, ServerName server, byte[] regionName, RegionLoadStats stats)
     */

    @Test
    public void testUpdateStatsAllNotNull() {
//      When StatisticTrackable, RegionLoadStats and regionName not null updateRegionstats should be called
        ResultStatsUtil.updateStats(tracker, serverName, regionName, stats);
        verify(tracker, times(1)).updateRegionStats(serverName, regionName, stats);
    }

    @Test
    public void testUpdateStatsRegionNameNull() {
//      regionName is null no call to updateRegionstats
        ResultStatsUtil.updateStats(tracker, serverName, null_regionName, stats);
        verify(tracker, never()).updateRegionStats(serverName, null_regionName, stats);
    }

    @Test
    public void testUpdateStatsRegionLoadStatsNull() {
//      RegionLoadStats is null no call to updateRegionstats
        ResultStatsUtil.updateStats(tracker, serverName, regionName, null_stats);
        verify(tracker, never()).updateRegionStats(serverName, regionName, null_stats);
    }

    @Test
    public void testUpdateStatsServerStatisticTrackerNull(){
//      StatisticTrackable is null no call to updateRegionstats but can't verify as StatisticTrackable is null
        ResultStatsUtil.updateStats(null_tracker, serverName, regionName, stats);
//        As StatisticTrackable is null not possible to verify if updateRegionStats is not called
        Assert.assertNull(null_tracker);
    }

    /**
     * Testing public static <T> T updateStats(T r, ServerStatisticTracker stats,
     *       HRegionLocation regionLocation)
     */

    @Test
    public void testUpdateStatsRegionLocationNotNull(){
//        HRegionLocation is not null with object other than Result obj
        Assert.assertNotNull(regionLocation);
        ServerStatisticTracker returned_object = ResultStatsUtil.updateStats(serverStatisticTracker, serverStatisticTracker, regionLocation);
        Assert.assertSame(returned_object, serverStatisticTracker);
    }

    @Test
    public void testUpdateStatsRegionLocationNull(){
//        HRegionLocation is null with object other than Result obj
        ServerStatisticTracker returned_object = ResultStatsUtil.updateStats(serverStatisticTracker, serverStatisticTracker, null_regionLocation);
        Assert.assertSame(returned_object, serverStatisticTracker);
    }

    @Test
    public void testUpdateStatsRegionLocationNotNullExpectResult(){
//        HRegionLocation is not null with passed parameter as Result obj
        Result returned_object = ResultStatsUtil.updateStats(result, serverStatisticTracker, regionLocation);
        Assert.assertSame(returned_object, result);
    }

    @Test
    public void testUpdateStatsRegionLocationNullExpectResult(){
//        HRegionLocation is null with passed parameter as Result obj
        Result returned_object = ResultStatsUtil.updateStats(result, serverStatisticTracker, null_regionLocation);
        Assert.assertSame(returned_object, result);
    }
}