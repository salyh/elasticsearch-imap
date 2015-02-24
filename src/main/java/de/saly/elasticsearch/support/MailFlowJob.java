/***********************************************************************************************************************
 *
 * Elasticsearch IMAP River - open source IMAP river for Elasticsearch
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
package de.saly.elasticsearch.support;

import java.io.IOException;
import java.util.regex.Pattern;

import javax.mail.MessagingException;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.UnableToInterruptJobException;

import de.saly.elasticsearch.mailsource.MailSource;
import de.saly.elasticsearch.river.imap.IMAPRiver;

//Disallow running multiple jobs based on this class at the same time.  
@DisallowConcurrentExecution
public class MailFlowJob implements InterruptableJob {

    private MailSource mailSource = null;
    private Pattern pattern;
    protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

    public MailFlowJob() {
        super();
    }

    public void execute() throws MessagingException, IOException {

        if (mailSource != null) {

            if (pattern == null) {
                mailSource.fetchAll(); // blocks
            } else {

                mailSource.fetch(pattern); // blocks

            }
        } else {
            throw new IllegalArgumentException("mailSource must not be null");
        }
    }

    @Override
    public void execute(final JobExecutionContext context) throws JobExecutionException {

        final JobKey key = context.getJobDetail().getKey();

        logger.debug("Executing mail flow job {}", key.toString());

        final JobDataMap data = context.getMergedJobDataMap();

        mailSource = (MailSource) data.get("mailSource");
        pattern = (Pattern) data.get("pattern");
        
        Client client = (Client) data.get("client");

        try {
            IMAPRiver.waitForYellowCluster(client);
            execute();
        } catch (final Exception e) {
            logger.error("Error in mail flow job {}: {} job", e, key.toString(), e.toString());
            final JobExecutionException e2 = new JobExecutionException(e);
            // this job will refire immediately
            // e2.refireImmediately();
            throw e2;
        }

        logger.debug("End of mail flow job with no errors {}", key.toString());

    }

    public MailSource getMailSource() {
        return mailSource;
    }

    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {

        if (mailSource != null) {
            mailSource.close();
        }

    }

    public void setMailSource(final MailSource mailSource) {
        this.mailSource = mailSource;
    }

    public void setPattern(final Pattern pattern) {
        this.pattern = pattern;
    }
    
    

}
