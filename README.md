elasticsearch-river-imap
========================

IMAP (and POP3) river for Elasticsearch

[![Build Status](https://travis-ci.org/salyh/elasticsearch-river-imap.png?branch=master)](https://travis-ci.org/salyh/elasticsearch-river-imap)

<a href="mailto:hendrikdev22@gmail.com">E-Mail hendrikdev22@gmail.com</a><p>
<a href="https://twitter.com/hendrikdev22">Twitter @hendrikdev22</a>

This river connects to IMAP4 or POP3 servers polls your mail and index it. The emails on the server will be never modified or removed from the server.
The river tracks (after the first initial full load) which mails are new or deleted and then only update the index for this mails.

Features:
* Incremental indexing of e-mails from a IMAP or POP3 server
* Support indexing of attachments (in conjunction with https://github.com/elasticsearch/elasticsearch-mapper-attachments/)
* Support for UTF-7 encoded e-mails (through jutf7)
* SSL, STARTTLS and SASL are supported (through JavaMail API)
* IMAP only: Folders which should be indexed can be specified with a regex pattern
* IMAP only: Subfolders can also be indexed (whole traversal of all folders)
* No special server capabilities needed
* Bulk indexing

The river acts currently as a disconnected client. This means that the river is polling and for every indexing run a new server connections are opened and, after work is done, closed.
At a later time is planned to use additionally the IMAP IDLE feature (if server supports it).

<h3>Installation</h3> 
(Until the first release is out you have to build this plugin yourself with maven or download from the github release page and install manually)

Branches:
* master for Elasticsearch 1.1.0 - 1.1.x

Prerequisites:
* Open JDK 6/7 or Oracle 7 JRE
* Elasticsearch 1.1.0 or higher
* At least one IMAP4 or POP3 server to connect to

Build yourself:
* Install maven
* execute ``mvn clean package -DskipTests=true`` 

``plugin.sh|bat -install river-imap -url http://...``

<h3>Configuration</h3>
<pre>curl -XPUT 'http://localhost:9200/_river/<name of your river>/_meta' -d '{

   "type":"imap",
   "mail.store.protocol":"imap",
   "mail.imap.host":"imap.server.com",
   "mail.imap.port":993,
   "mail.imap.ssl.enable":true,
   "mail.imap.connectionpoolsize":"3",
   "mail.debug":"false",
   "mail.imap.timeout":10000,
   "user":"user@domain.com",
   "password":"secret",
   "schedule":null,
   "interval":"60s",
   "threads":5,
   "folderpattern":null,
   "bulk_size":100,
   "max_bulk_requests":"30",
   "bulk_flush_interval":"5s",
   "mail_index_name":"imapriverdata",
   "mail_type_name":"mail",
   "with_striptags_from_textcontent":true,
   "with_attachments":false,
   "with_text_content":true
   
}'</pre>

* ``type`` - always "imap"
* ``mail.*`` - see JAVAMail documentation https://javamail.java.net/nonav/docs/api/  (default: none)
* ``user`` - user name for server login (default: ``null``)
* ``password`` - password for server login (default: ``null``)
* ``schedule`` - a cron expression like ``0/3 0-59 0-23 ? * *`` (default: ``null``)
* ``interval`` - if no ``schedule`` is set then this is will be the indexing interval (default: ``60s``)
* ``threads`` - How many thready for parallel indexing (must be 1 or higher) (default: ``5``)
* ``folderpattern`` - IMAP only: regular expressions which folders should be indexed (default: ``null``)
* ``bulk_size`` - the length of each bulk index request submitted (default: ``100``)
* ``max_bulk_requests`` - the maximum number of concurrent bulk requests (default: ``30``)
* ``bulk_flush_interval`` - the time period the bulk processor is flushing outstanding documents (default: ``5s``)
* ``mail_index_name`` - name of the index which holds the mail (default: ``imapriverdata``)
* ``mail_type_name`` - name of the type (default: ``mail``)
* ``with_striptags_from_textcontent`` - if ``true`` then html/xml tags are stripped from text content (default: ``true``)
* ``with_attachments`` - if ``true`` then attachments will be indexed (default: ``false``)
* ``with_text_content`` - if ``true`` then the text content of the mail is indexed (default: ``true``)

Note: For POP3 only the "INBOX" folder is supported. This is a limitation of the POP3 protocol.