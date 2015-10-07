/***********************************************************************************************************************
 *
 * Elasticsearch IMAP/Pop3 E-Mail Importer
 * ==========================================
 *
 * Copyright (C) 2014 by Hendrik Saly (http://saly.de) and others.
 * 
 * Contains (partially) copied code from Jörg Prante's Elasticsearch JDBC river (https://github.com/jprante/elasticsearch-river-jdbc)
 *
 ***********************************************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 ***********************************************************************************************************************
 *
 * $Id:$
 *
 **********************************************************************************************************************/
package de.saly.elasticsearch.importer.imap.state;

import java.util.Date;

public class State {

    private boolean exists;

    private String folderUrl;

    private long lastCount = -1;

    private Date lastIndexed;

    private Date lastSchedule;

    private long lastTook = -1;

    private long lastUid = -1;

    private Long uidValidity;

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final State other = (State) obj;
        if (folderUrl == null) {
            if (other.folderUrl != null) {
                return false;
            }
        } else if (!folderUrl.equals(other.folderUrl)) {
            return false;
        }
        return true;
    }

    public String getFolderUrl() {
        return folderUrl;
    }

    public long getLastCount() {
        return lastCount;
    }

    public Date getLastIndexed() {
        return lastIndexed;
    }

    public Date getLastSchedule() {
        return lastSchedule;
    }

    public long getLastTook() {
        return lastTook;
    }

    public long getLastUid() {
        return lastUid;
    }

    public Long getUidValidity() {
        return uidValidity;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (folderUrl == null ? 0 : folderUrl.hashCode());
        return result;
    }

    public boolean isExists() {
        return exists;
    }

    public void setExists(final boolean exists) {
        this.exists = exists;
    }

    public void setFolderUrl(final String folderUrl) {
        this.folderUrl = folderUrl;
    }

    public void setLastCount(final long lastCount) {
        this.lastCount = lastCount;
    }

    public void setLastIndexed(final Date lastIndexed) {
        this.lastIndexed = lastIndexed;
    }

    public void setLastSchedule(final Date lastSchedule) {
        this.lastSchedule = lastSchedule;
    }

    public void setLastTook(final long lastTook) {
        this.lastTook = lastTook;
    }

    public void setLastUid(final long lastUid) {
        this.lastUid = lastUid;
    }

    public void setUidValidity(final Long uidValidity) {
        this.uidValidity = uidValidity;
    }

    @Override
    public String toString() {
        return "RiverState [folderUrl=" + folderUrl + ", uidValidity=" + uidValidity + ", lastUid=" + lastUid + ", lastSchedule="
                + lastSchedule + ", lastIndexed=" + lastIndexed + ", lastTook=" + lastTook + ", exists=" + exists + ", lastCount="
                + lastCount + "]";
    }

}
