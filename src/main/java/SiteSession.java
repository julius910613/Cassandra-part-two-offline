/*
Copyright 2014 Red Hat, Inc. and/or its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.util.concurrent.TimeUnit;

/**
 * Unit tests for the SiteSession class
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2014-01
 */
public class SiteSession {

    public static long MAX_IDLE_MS = TimeUnit.MINUTES.toMillis(30);
    private static long globalLastHitMillis;

    private String id;
    private long firstHitMillis;
    private HyperLogLog hyperLogLog = new HyperLogLog(0.05);
    private long lastHitMillis;
    private long hitCount = 0;
    private boolean timeout = false;
    private boolean outOfOrder = false;
    private boolean flagNormal = false;

    /**
     * Creates a new SiteSession instance based on its first hit.
     *
     * @param id             the session id
     * @param firstHitMillis the time of the first hit in the session, in milliseconds since unix epoch
     * @param url            the url of the first hit
     */
    public SiteSession(String id, long firstHitMillis, String url) {
        this.id = id;
        this.firstHitMillis = firstHitMillis;
        update(firstHitMillis, url);
    }

    public SiteSession() {

    }

    /**
     * Reset the global session 'clock'. Intended only for test use
     */
    public static void resetGlobalMax() {
        globalLastHitMillis = 0;
    }

    public static void setGlobalLastHitMillis(long hitMillis) {
        globalLastHitMillis = hitMillis;
    }

    public void resetHyperloglog() {
        hyperLogLog = new HyperLogLog(0.05);
    }

    public void resetHitCount() {
        hitCount = 0;
    }

    public void reset() {
        hyperLogLog = new HyperLogLog(0.05);
        hitCount = 0;
        timeout = false;

        outOfOrder = false;

        flagNormal = false;
    }

    public boolean getflagNormal() {
        return flagNormal;
    }

    public boolean getTimeOut() {
        return timeout;
    }

    public boolean getOutOfOrder() {
        return outOfOrder;
    }

    public void setID(String ID) {
        id = ID;
    }

    public String getId() {
        return id;
    }

    public long getFirstHitMillis() {
        return firstHitMillis;
    }

    public void setFirstHitMillis(long millis) {
        firstHitMillis = millis;
    }

    public long getLastHitMillis() {
        return lastHitMillis;
    }

    public long getHitCount() {
        return hitCount;
    }

    public HyperLogLog getHyperLogLog() {
        return hyperLogLog;
    }

    /**
     * Modify the session by adding a new hit.
     *
     * @param hitMillis the time of the hit in the session, in milliseconds since unix epoch
     *                  //@param url the url of the hit
     *                  <p/>
     *                  //@throws IllegalArgumentException if the time is less that the global max
     *                  //or after the session's timeout
     */
    public void update(long hitMillis, String url) {

        if (lastHitMillis > 0 && lastHitMillis + MAX_IDLE_MS < hitMillis) {

            //throw new IllegalArgumentException("interval since last hit exceeds session timeout");
            timeout = true;
        }
        this.lastHitMillis = hitMillis;

        if (hitMillis < globalLastHitMillis) {
            outOfOrder = true;

            //throw new IllegalArgumentException("hit processed out of order");
        }
        //globalLastHitMillis = hitMillis;

        //hitCount++;
        // hyperLogLog.offer(url);

        if (timeout == false && outOfOrder == false) {

            flagNormal = true;

        }


    }

    public void addHitCount(long hitMillis, String url) {

        timeout = false;

        outOfOrder = false;

        flagNormal = false;

        globalLastHitMillis = hitMillis;

        hitCount++;

        hyperLogLog.offer(url);
    }

    /**
     * Returns true if the global last hit (i.e. virtual clock) has advanced such that
     * any in-order update to this session would now exceed its timeout threshold
     *
     * @return true if the session has expired, false otherwise
     */
    public boolean isExpired() {
        return globalLastHitMillis - lastHitMillis > MAX_IDLE_MS;
    }

}