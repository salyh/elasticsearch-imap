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

Branches:
* master for Elasticsearch 1.2.x/1.3.x

<h3>Installation</h3> 
Prerequisites:
* Open JDK 7 or Oracle 7 JRE (Java 6 is no longer supported and Java 8 is untested)
* Elasticsearch 1.2-1.3 or higher
* At least one IMAP4 or POP3 server to connect to

``plugin.sh|.bat -i river-imap -u http://dl.bintray.com/salyh/maven/de/saly/elasticsearch/plugin/elasticsearch-river-imap/0.3/elasticsearch-river-imap-0.3-plugin.zip``

<h3>Configuration</h3>
<pre>curl -XPUT 'http://localhost:9200/_river/nameofyourriver/_meta' -d '{

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
   "with_text_content":true,
   "with_flag_sync":true,
   "index_settings" : null,
   "type_mapping" : null
   
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
* ``with_flag_sync`` - IMAP only: if ``true`` then message flag changes will be detected and indexed. Maybe slow for very huge mailboxes. (default: ``true``)
* ``index_settings`` - optional settings for the Elasticsearch index
* ``type_mapping`` - optional mapping for the Elasticsearch index type
* ``headers_to_fields`` - array with e-mail header names to include as proper fields. To create a legal field name, the header name is prefixed with ``header_``, lowercased and has all non-alphanumeric characters replaced with ``_``. For example, an input of ``["Message-ID"]`` will copy that header into a field with name ``header_message_id``.

Note: For POP3 only the "INBOX" folder is supported. This is a limitation of the POP3 protocol.

<h3>Default Mapping Example</h3>
```json
"mail" : {
        "properties" : {
          "attachmentCount" : {
            "type" : "long"
          },
          "bcc" : {
            "properties" : {
              "email" : {
                "type" : "string"
              },
              "personal" : {
                "type" : "string"
              }
            }
          },
          "cc" : {
            "properties" : {
              "email" : {
                "type" : "string"
              },
              "personal" : {
                "type" : "string"
              }
            }
          },
          "contentType" : {
            "type" : "string"
          },
          "flaghashcode" : {
            "type" : "integer"
          },
          "flags" : {
            "type" : "string"
          },
          "folderFullName" : {
            "type" : "string",
            "index" : "not_analyzed"
          },
          "folderUri" : {
            "type" : "string"
          },
          "from" : {
            "properties" : {
              "email" : {
                "type" : "string"
              },
              "personal" : {
                "type" : "string"
              }
            }
          },
          "headers" : {
            "properties" : {
              "name" : {
                "type" : "string"
              },
              "value" : {
                "type" : "string"
              }
            }
          },
          "mailboxType" : {
            "type" : "string"
          },
          "receivedDate" : {
            "type" : "date",
            "format" : "basic_date_time"
          },
          "sentDate" : {
            "type" : "date",
            "format" : "basic_date_time"
          },
          "size" : {
            "type" : "long"
          },
          "subject" : {
            "type" : "string"
          },
          "textContent" : {
            "type" : "string"
          },
          "to" : {
            "properties" : {
              "email" : {
                "type" : "string"
              },
              "personal" : {
                "type" : "string"
              }
            }
          },
          "uid" : {
            "type" : "long"
          }
        }
      }
    }
```

<h3>Content Example</h3>
```json
{
      "_index" : "imapriverdata",
      "_type" : "mail",
      "_id" : "50220::imap://test%40xxx.com@imap.strato.de/import",
      "_score" : 1.0, "_source" : {
  "attachmentCount" : 0,
  "attachments" : null,
  "bcc" : null,
  "cc" : null,
  "contentType" : "text/plain; charset=ISO-8859-15",
  "flaghashcode" : 16,
  "flags" : [ "Recent" ],
  "folderFullName" : "test",
  "folderUri" : "imap://test%40xxx.com@imap.strato.de/import",
  "from" : {
    "email" : "suchagent@isrch.de",
    "personal" : null
  },
  "headers" : [ {
    "name" : "Subject",
    "value" : "Suchagent Wohnung mieten in Berlin -  1 neues Objekt gefunden!"
  }, {
    "name" : "Return-Path",
    "value" : "<suchagent@isrch.de>"
  }, {
    "name" : "Content-Transfer-Encoding",
    "value" : "quoted-printable"
  }, {
    "name" : "To",
    "value" : "sss@ddd.org"
  }, {
    "name" : "X-OfflineIMAP-1722382714-52656d6f7465-6165727a7465",
    "value" : "1248516496-0146849121575-v5.99.4"
  }, {
    "name" : "Message-ID",
    "value" : "<8277550.1132283844462.JavaMail.noreply@isrch.de>"
  }, {
    "name" : "Mime-Version",
    "value" : "1.0"
  }, {
    "name" : "X-Gmail-Labels",
    "value" : "ablage,hendrik.yyy@gmx.de"
  }, {
    "name" : "X-GM-THRID",
    "value" : "1309162987234255956"
  }, {
    "name" : "Delivered-To",
    "value" : "GMX delivery to sss@ddd.org"
  }, {
    "name" : "Reply-To",
    "value" : "suchagent@isrch.de"
  }, {
    "name" : "Date",
    "value" : "Fri, 18 Nov 2005 04:17:24 +0100 (MET)"
  }, {
    "name" : "Auto-Submitted",
    "value" : "auto-generated"
  }, {
    "name" : "Received",
    "value" : "(qmail invoked by alias); 18 Nov 2005 03:17:25 -0000"
  }, {
    "name" : "Content-Type",
    "value" : "text/plain; charset=\"ISO-8859-15\""
  }, {
    "name" : "From",
    "value" : "suchagent@isrch.de"
  } ],
  "mailboxType" : "IMAP",
  "popId" : null,
  "receivedDate" : 1132283845000,
  "sentDate" : 1132283844000,
  "size" : 3645,
  "subject" : "Suchagent Wohnung mieten in Berlin -  1 neues Objekt gefunden!",
  "textContent" : "Sehr geehrter Nutzer, ... JETZT AUCH IM FERNSEHEN: IMMOBILIENANGEBOTE FÜR HAMBURG UND UMGEBUNG!\r\n\tFinden Sie Ihre Wunschwohnung oder  ..."
  "to" : [ {
    "email" : "sss@ddd.org",
    "personal" : null
  } ],
  "uid" : 50220
}
    } 
```

<h3>Indexing attachments</h3> 
If you want also indexing your mail attachments follow instructions here:
https://github.com/salyh/elasticsearch-river-imap/issues/10#issuecomment-50125929


<h3>Contributers/Credits</h3> 
* Hans Jørgen Hoel (https://github.com/hansjorg)
* Stefan Thies (https://github.com/megastef)

<h3>License</h3> 
Copyright (C) 2014 by Hendrik Saly (http://saly.de) and others.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
