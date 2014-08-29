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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.mail.BodyPart;
import javax.mail.Header;
import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.UIDFolder;
import javax.mail.internet.InternetAddress;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import com.sun.mail.pop3.POP3Folder;

public class IndexableMailMessage {

    protected static final ESLogger logger = ESLoggerFactory.getLogger(IndexableMailMessage.class.getName());

    public static IndexableMailMessage fromJavaMailMessage(final Message jmm, final boolean withTextContent, final boolean withHtmlContent, final boolean preferHtmlContent, final boolean withAttachments,
            final boolean stripTags, List<String> headersToFields) throws MessagingException, IOException {
        final IndexableMailMessage im = new IndexableMailMessage();

        @SuppressWarnings("unchecked")
        final Enumeration<Header> allHeaders = jmm.getAllHeaders();

        final Set<IndexableHeader> headerList = new HashSet<IndexableHeader>();
        while (allHeaders.hasMoreElements()) {
            final Header h = allHeaders.nextElement();
            headerList.add(new IndexableHeader(h.getName(), h.getValue()));
        }

        im.setHeaders(headerList.toArray(new IndexableHeader[headerList.size()]));

        im.setSelectedHeaders(extractHeaders(im.getHeaders(), headersToFields));

        if (jmm.getFolder() instanceof POP3Folder) {
            im.setPopId(((POP3Folder) jmm.getFolder()).getUID(jmm));
            im.setMailboxType("POP");

        } else {
            im.setMailboxType("IMAP");
        }

        if (jmm.getFolder() instanceof UIDFolder) {
            im.setUid(((UIDFolder) jmm.getFolder()).getUID(jmm));
        }

        im.setFolderFullName(jmm.getFolder().getFullName());

        im.setFolderUri(jmm.getFolder().getURLName().toString());

        im.setContentType(jmm.getContentType());
        im.setSubject(jmm.getSubject());
        im.setSize(jmm.getSize());
        im.setSentDate(jmm.getSentDate());

        if (jmm.getReceivedDate() != null) {
            im.setReceivedDate(jmm.getReceivedDate());
        }

        if (jmm.getFrom() != null && jmm.getFrom().length > 0) {
            im.setFrom(Address.fromJavaMailAddress(jmm.getFrom()[0]));
        }

        if (jmm.getRecipients(RecipientType.TO) != null) {
            im.setTo(Address.fromJavaMailAddress(jmm.getRecipients(RecipientType.TO)));
        }

        if (jmm.getRecipients(RecipientType.CC) != null) {
            im.setCc(Address.fromJavaMailAddress(jmm.getRecipients(RecipientType.CC)));
        }

        if (jmm.getRecipients(RecipientType.BCC) != null) {
            im.setBcc(Address.fromJavaMailAddress(jmm.getRecipients(RecipientType.BCC)));
        }

        if (withTextContent) {

            // try {

            String textContent = getText(jmm, 0, false);

            if (stripTags) {
                textContent = stripTags(textContent);
            }

            im.setTextContent(textContent);
            // } catch (final Exception e) {
            // logger.error("Unable to retrieve text content for message {} due to {}",
            // e, ((MimeMessage) jmm).getMessageID(), e);
            // }
        }

        if (withHtmlContent) { 

            // try {

            String htmlContent = getText(jmm, 0, preferHtmlContent);

            im.setHtmlContent(htmlContent);
            // } catch (final Exception e) {
            // logger.error("Unable to retrieve text content for message {} due to {}",
            // e, ((MimeMessage) jmm).getMessageID(), e);
            // }
        }

        if (withAttachments) {

            try {
                final Object content = jmm.getContent();

                // look for attachments
                if (jmm.isMimeType("multipart/*") && content instanceof Multipart) {
                    List<ESAttachment> attachments = new ArrayList<ESAttachment>();

                    final Multipart multipart = (Multipart) content;

                    for (int i = 0; i < multipart.getCount(); i++) {
                        final BodyPart bodyPart = multipart.getBodyPart(i);
                        if (!Part.ATTACHMENT.equalsIgnoreCase(bodyPart.getDisposition()) && !StringUtils.isNotBlank(bodyPart.getFileName())) {
                            continue; // dealing with attachments only
                        }
                        final InputStream is = bodyPart.getInputStream();
                        final byte[] bytes = IOUtils.toByteArray(is);
                        IOUtils.closeQuietly(is);
                        attachments.add(new ESAttachment(bodyPart.getContentType(), bytes, bodyPart.getFileName()));
                    }

                    if (!attachments.isEmpty()) {
                        im.setAttachments(attachments.toArray(new ESAttachment[attachments.size()]));
                        im.setAttachmentCount(im.getAttachments().length);
                        attachments.clear();
                        attachments = null;
                    }

                }
            } catch (final Exception e) {
                logger.error("Error indexing attachments (message will be indexed but without attachments) due to {}", e, e.toString());
            }

        }

        im.setFlags(IMAPUtils.toStringArray(jmm.getFlags()));
        im.setFlaghashcode(jmm.getFlags().hashCode());

        return im;
    }

    private static Map<String, String> extractHeaders(
            IndexableHeader[] allHeaders, List<String> headersToFields) {

        Map<String, String> map = new HashMap<>();
        for(String headerName : headersToFields) {
            for(IndexableHeader header : allHeaders) {
                // e-mail headers are case insensitive
                if(headerName.toLowerCase().equals(header.getName().toLowerCase())) {
                    String fieldName = "header_" + headerName.replaceAll("[^A-Za-z0-9 ]", "_").toLowerCase();
                    map.put(fieldName, header.getValue());
                    break;
                }
            }
        }
        return map;
    }


    private static String getText(final Part p, int depth, final boolean preferHtmlContent) throws MessagingException, IOException {

        if (depth >= 100) {
            throw new IOException("Endless recursion detected ");
        }

        // TODO fix encoding for buggy encoding headers

        if (p.isMimeType("text/*")) {

            Object content = null;
            try {
                content = p.getContent();
            } catch (final Exception e) {
                logger.error("Unable to index the content of a message due to {}", e.toString());
                return null;
            }

            if (content instanceof String) {
                final String s = (String) p.getContent();
                return s;

            }

            if (content instanceof InputStream) {

                final InputStream in = (InputStream) content;
                // TODO guess encoding with
                // http://code.google.com/p/juniversalchardet/
                final String s = IOUtils.toString(in, "UTF-8");
                IOUtils.closeQuietly(in);
                return s;

            }

            throw new MessagingException("Unknown content class representation: " + content.getClass());

        }

        if (p.isMimeType("multipart/alternative")) {
            // prefer plain text over html text
            final Multipart mp = (Multipart) p.getContent();
            String text = null;
            for (int i = 0; i < mp.getCount(); i++) {
                final Part bp = mp.getBodyPart(i);
                if (bp.isMimeType("text/html")) {
                    if (text == null) {
                        text = getText(bp, ++depth, preferHtmlContent);
                    }
		    if (preferHtmlContent) {
                        return text;
                    } else {
                        continue;
                    }
                } else if (bp.isMimeType("text/plain")) {
                    final String s = getText(bp, ++depth, preferHtmlContent);
                    if (s != null && !preferHtmlContent) {
                        return s;
                    } else {
                        continue;
                    }
                } else {
                    return getText(bp, ++depth, preferHtmlContent);
                }
            }
            return text;
        } else if (p.isMimeType("multipart/*")) {
            final Multipart mp = (Multipart) p.getContent();
            for (int i = 0; i < mp.getCount(); i++) {
                final String s = getText(mp.getBodyPart(i), ++depth, preferHtmlContent);
                if (s != null) {
                    return s;
                }
            }
        }

        return null;
    }

    private static String stripTags(final String text) {
        if (text == null) {
            return null;
        }

        return text.replaceAll("\\<.*?\\>", "");
    }

    private int attachmentCount;

    private ESAttachment[] attachments;

    private Address[] bcc;

    private Address[] cc;

    private String contentType;

    private int flaghashcode;

    private String[] flags;

    private String folderFullName;

    private String folderUri;

    private Address from;

    private IndexableHeader[] headers;

    private Map<String, String> selectedHeaders;

    private String mailboxType;

    private final ObjectMapper mapper = new ObjectMapper();

    private String popId;

    private Date receivedDate;

    private Date sentDate;

    private int size;

    private String subject;

    private String textContent;

    private String htmlContent;

    private Address[] to;

    private long uid;

    public IndexableMailMessage() {
        mapper.enable(SerializationConfig.Feature.INDENT_OUTPUT);
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    }

    public String build() throws IOException {
        return mapper.writeValueAsString(this);
    }

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
        final IndexableMailMessage other = (IndexableMailMessage) obj;
        if (folderFullName == null) {
            if (other.folderFullName != null) {
                return false;
            }
        } else if (!folderFullName.equals(other.folderFullName)) {
            return false;
        }
        if (popId == null) {
            if (other.popId != null) {
                return false;
            }
        } else if (!popId.equals(other.popId)) {
            return false;
        }
        if (uid != other.uid) {
            return false;
        }
        return true;
    }

    public int getAttachmentCount() {
        return attachmentCount;
    }

    public ESAttachment[] getAttachments() {
        return attachments;
    }

    public Address[] getBcc() {
        return bcc;
    }

    public Address[] getCc() {
        return cc;
    }

    public String getContentType() {
        return contentType;
    }

    public int getFlaghashcode() {
        return flaghashcode;
    }

    public String[] getFlags() {
        return flags;
    }

    public String getFolderFullName() {
        return folderFullName;
    }

    public String getFolderUri() {
        return folderUri;
    }

    public Address getFrom() {
        return from;
    }

    public IndexableHeader[] getHeaders() {
        return headers;
    }

    @JsonAnyGetter
    public Map<String,String> getSelectedHeaders() {
        return selectedHeaders;
    }

    public String getMailboxType() {
        return mailboxType;
    }

    public String getPopId() {
        return popId;
    }

    public Date getReceivedDate() {
        return receivedDate;
    }

    public Date getSentDate() {
        return sentDate;
    }

    public int getSize() {
        return size;
    }

    public String getSubject() {
        return subject;
    }

    public String getTextContent() {
        return textContent;
    }

    public String getHtmlContent() {
        return htmlContent;
    }

    public Address[] getTo() {
        return to;
    }

    public long getUid() {
        return uid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (folderFullName == null ? 0 : folderFullName.hashCode());
        result = prime * result + (popId == null ? 0 : popId.hashCode());
        result = prime * result + (int) (uid ^ uid >>> 32);
        return result;
    }

    public void setAttachmentCount(final int attachmentCount) {
        this.attachmentCount = attachmentCount;
    }

    public void setAttachments(final ESAttachment[] attachments) {
        this.attachments = attachments;
    }

    public void setBcc(final Address[] bcc) {
        this.bcc = bcc;
    }

    public void setCc(final Address[] cc) {
        this.cc = cc;
    }

    public void setContentType(final String contentType) {
        this.contentType = contentType;
    }

    public void setFlaghashcode(final int flaghashcode) {
        this.flaghashcode = flaghashcode;
    }

    public void setFlags(final String[] flags) {
        this.flags = flags;
    }

    public void setFolderFullName(final String folderFullName) {
        this.folderFullName = folderFullName;
    }

    public void setFolderUri(final String folderUri) {
        this.folderUri = folderUri;
    }

    public void setFrom(final Address from) {
        this.from = from;
    }

    public void setHeaders(final IndexableHeader[] headers) {
        this.headers = headers;
    }

    public void setSelectedHeaders(Map<String, String> selectedHeaders) {
        this.selectedHeaders = selectedHeaders;
    }

    public void setMailboxType(final String mailboxType) {
        this.mailboxType = mailboxType;
    }

    public void setPopId(final String popId) {
        this.popId = popId;
    }

    public void setReceivedDate(final Date receivedDate) {
        this.receivedDate = receivedDate;
    }

    public void setSentDate(final Date sentDate) {
        this.sentDate = sentDate;
    }

    public void setSize(final int size) {
        this.size = size;
    }

    public void setSubject(final String subject) {
        this.subject = subject;
    }

    public void setTextContent(final String textContent) {

        this.textContent = textContent;
    }

    public void setHtmlContent(final String htmlContent) {

        this.htmlContent = htmlContent;
    }

    public void setTo(final Address[] to) {
        this.to = to;
    }

    public void setUid(final long uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "IndexableMailMessage [uid=" + uid + ", headers=" + headers + ", contentType=" + contentType + ", subject=" + subject
                + ", size=" + size + ", sentDate=" + sentDate + ", receivedDate=" + receivedDate + ", from=" + from + ", to="
                + Arrays.toString(to) + ", cc=" + Arrays.toString(cc) + ", bcc=" + Arrays.toString(bcc) + "]";
    }

    public static class Address {
        public static Address fromJavaMailAddress(final javax.mail.Address jma) {
            final Address a = new Address();
            final InternetAddress ia = (InternetAddress) jma;
            a.setEmail(ia.getAddress());
            a.setPersonal(ia.getPersonal());

            return a;
        }

        public static Address[] fromJavaMailAddress(final javax.mail.Address[] jmas) {
            final Address[] as = new Address[jmas.length];
            int i = 0;
            for (final javax.mail.Address jma : jmas) {

                final Address a = new Address();
                final InternetAddress ia = (InternetAddress) jma;
                a.setEmail(ia.getAddress());
                a.setPersonal(ia.getPersonal());
                as[i++] = a;
            }

            return as;
        }

        private String email;

        private String personal;

        public Address() {
            super();
        }

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
            final Address other = (Address) obj;
            if (email == null) {
                if (other.email != null) {
                    return false;
                }
            } else if (!email.equals(other.email)) {
                return false;
            }
            return true;
        }

        public String getEmail() {
            return email;
        }

        public String getPersonal() {
            return personal;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (email == null ? 0 : email.hashCode());
            return result;
        }

        public void setEmail(final String email) {
            this.email = email;
        }

        public void setPersonal(final String personal) {
            this.personal = personal;
        }

        @Override
        public String toString() {
            return "Address [email=" + email + ", personal=" + personal + "]";
        }

    }

    public static class ESAttachment {
        private byte[] content;
        private String contentType;
        private String fileName;
        private int size;

        public ESAttachment() {

        }

        public ESAttachment(final String contentType, final byte[] bytes, final String filename) {
            super();
            setContentType(contentType);
            setContent(bytes);
            setFilename(filename);
        }

        public byte[] getContent() {
            return content;
        }

        public String getContentType() {
            return contentType;
        }

        public String getFilename() {
            return fileName;
        }

        public String getName() {
            return fileName;
        }

        public int getSize() {
            return size;
        }

        public void setContent(final byte[] content) {
            this.content = content;
            this.size = content != null ? content.length : 0;
        }

        public void setContentType(final String contentType) {
            this.contentType = contentType;
        }

        public void setFilename(final String filename) {
            this.fileName = filename;
        }

        public void setName(final String name) {
            this.fileName = name;
        }

        public void setSize(final int size) {
            this.size = size;
        }

    }

    public static class IndexableHeader {
        final String name;
        final String value;

        public IndexableHeader(final String name, final String value) {
            super();
            this.name = name;
            this.value = value;
        }

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
            final IndexableHeader other = (IndexableHeader) obj;
            if (name == null) {
                if (other.name != null) {
                    return false;
                }
            } else if (!name.equals(other.name)) {
                return false;
            }
            return true;
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (name == null ? 0 : name.hashCode());
            return result;
        }

    }

}
