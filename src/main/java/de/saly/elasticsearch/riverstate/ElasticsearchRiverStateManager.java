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
package de.saly.elasticsearch.riverstate;



import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

public class ElasticsearchRiverStateManager implements RiverStateManager{

	protected final ESLogger logger = ESLoggerFactory
			.getLogger(this.getClass().getName());
	private static final String RIVERSTATE_TYPE = "imapriverstate";
	private static final String FOLDERSTATE_ID = "folderstate";
	private static final String ERRORS_ID = "errors";
	private final ObjectMapper mapper = new ObjectMapper();
	private Client client;
	private String index;
	
	
	public ElasticsearchRiverStateManager client(final Client client) {
		this.client = client;
		return this;
	}
	
	public ElasticsearchRiverStateManager index(final String index) {
		this.index = index;
		return this;
	}
	
	public String index() {
		return index;
	}
	
	
	@Override
	public synchronized RiverState getRiverState(final Folder folder)
			throws MessagingException {

		try {

			if (client.admin().indices().prepareExists(index()).execute()
					.actionGet().isExists()) {
				
				final GetResponse response = client
						.prepareGet(index(), RIVERSTATE_TYPE , FOLDERSTATE_ID+"_"+folder.getURLName().toString().hashCode())
						.execute().get();
				
				if(!response.isSourceEmpty())
				{
				 return mapper.readValue(
						response.getSourceAsString(),
						new TypeReference<RiverState>() {
						});

				
				}
			}
		} catch (Exception ex) {
			throw new MessagingException("Unable to get river state", ex);
		} 

		
		RiverState rs = new RiverState();
		rs.setFolderUrl(folder.getURLName().toString());
		rs.setLastUid(1L);
		rs.setExists(true);
		return rs;

	}
	
	
	@Override
	public  void setRiverState(final RiverState state)
			throws MessagingException {

		try {
			 logger.debug("set riverstate "+state);
			
			 client
					.prepareIndex(index(), RIVERSTATE_TYPE , FOLDERSTATE_ID+"_"+state.getFolderUrl().hashCode())
					.setSource(mapper.writeValueAsString(state)).execute()
					.actionGet();
			 
			 logger.debug("set riverstate done");
		} catch (Exception ex) {
			throw new MessagingException("Unable to set river state", ex);
		} 

	}

	
	@Override
	public  void onError(String errmsg, final Folder folder, final Exception e) {

		logger.error("Folder " + folder.getFullName() + " throws an error:" + errmsg+e,e);

		try {
			client
					.prepareIndex(index(), RIVERSTATE_TYPE , ERRORS_ID+"_"+folder.getURLName().toString().hashCode())
					.setSource(folder.getURLName().toString(), errmsg+e).execute()
					.actionGet();
			
			
			
		} catch (Exception ex) {
			logger.error("Unable to log an error because of "+ex+errmsg, e);
		} 

	}

	@Override
	public  void onError(String errmsg,Message msg, Exception e) {
		

		try {
			logger.error("Message " + ((MimeMessage)msg).getMessageID() + " throws an error: "  +errmsg+ e,e);
			
			client
					.prepareIndex(index(), RIVERSTATE_TYPE , ERRORS_ID+"_"+((MimeMessage)msg).getMessageID().hashCode())
					.setSource(((MimeMessage)msg).getMessageID(), errmsg+e).execute()
					.actionGet();
			
			
			
		} catch (Exception ex) {
			logger.error("Unable to log an error because of "+ex+errmsg, e);
		} 
	}
	
	

}
