# Getting started with IBM DB Django adapter 

IBM DB2 support for the Django application Framework

ibm_db_django enables access to IBM DB2 from Django applications
[http://www.djangoproject.com/]

The adapter has been developed and is supported by IBM

# Prerequisites for Django on Python 

 * Install Python 2.7 or Python 3 <= 3.11.4.
   The minimum python version supported by driver is python 2.7 and the latest version supported is python 3.11.4 except version 3.3 as it has reached end-of-life.
 * Django Framework 2.2.0 or 3.1 or 3.2 or 4.2
 * regex package version >= 2020.7.14 
 * IBM_DB driver and IBM_DB_DBI wrapper 1.0 or higher
   ``` 
    Install ibm_db driver with below commands:
	    Linux and Windows: 
	      pip install ibm_db
	    Mac:
   	    pip install --no-cache-dir ibm_db
   ```
 
# Installation 

## 1. Install Django 

Install Django as per instructions from the Django [http://docs.djangoproject.com/en/dev/topics/install/#installing-an-official-release website].

 * For 1.0.2, you need to apply a patch in django in-order to remove Non-standard SQL generation issue. 
 * The patch is located at http://code.djangoproject.com/ticket/9862.
 * You can extract creation.py file from http://code.djangoproject.com/changeset/9703?format=zip&new=9703 and paste it to /django/db/backends/
 * For versions greater than 1.0.2 no patch is required.

## 2. Install DB2 Django adapter (ibm_db_django)  

```  
For Django 2.2,
	$ pip install ibm_db_django==1.3.0.0  
	(or)
For Django 3.1,
	$ pip install ibm_db_django==1.4.0.0  
	(or)
For Django 3.2,
	$ pip install ibm_db_django>=1.5.0.0  
For Django 4.2,
	$ pip install ibm_db_django>=1.5.3.0
```
 
# Tested Operating Systems 

 * Ubuntu Linux 7.04 64 bit
 * Win64/Win32 bit
 * Mac OS

# Supported Databases 

 * IBM DB2 Database for Linux, Unix and Windows, version 8.2 or higher.

# Future Supported Databases 

 * IBM Cloudscape
 * Apache Derby
 * IBM Informix Cheetah version 11.10 onwards
 * Remote connections to i5/OS (iSeries)

# Testing 
```
 * Create a new Django project by executing "django-admin.py startproject myproj".
 * Now go to this newly create directory, and edit settings.py file to access DB2.
 * In case of nix the steps will be like:
  {{{
  $ django-admin.py startproject myproj
  $ cd myproj
  $ vi settings.py
  }}}
 * The settings.py will be like (after adding DB2 properties):
   {{{
   DATABASES = {
      'default': {
         'ENGINE'     : 'ibm_db_django',
         'NAME'       : 'mydb',
         'USER'       : 'db2inst1',
         'PASSWORD'   : 'ibmdb2',
         'HOST'       : 'localhost',
         'PORT'       : '50000',
         'PCONNECT'   :  True,      #Optional property, default is false
      }
   }
   }}}
   
 * Change USE_TZ to False
 
 * RUN python manage.py migrate
 
 * In the tuple INSTALLED_APPS in settings.py add the following lines:
   {{{
   'django.contrib.flatpages',
   'django.contrib.redirects',
   'django.contrib.comments',
   'django.contrib.admin',
   }}}
 * Next step is to run a simple test suite. To do this just execute following command in the project we created earlier:
   {{{
   $ python manage.py test #for Django-1.5.x or older
   $ Python manage.py test django.contrib.auth #For Django-1.6.x onwards, since test discovery behavior have changed
   }}} 
 * For Windows, steps are same as above. In case of editing settings.py file, use notepad (or any other) editor.
```
# Database Transactions 

 *  Django by default executes without transactions i.e. in auto-commit mode. This default is generally not what you want in web-applications. [http://docs.djangoproject.com/en/dev/topics/db/transactions/ Remember to turn on transaction support in Django]

# Known Limitations of ibm_db_django adapter 

 * Non-standard SQL queries are not supported. e.g. "SELECT ? FROM TAB1"
 * dbshell will not work if server is remote and client is DB2 thin client.
 * For updations involving primary/foreign key references, the entries should be made in correct order. Integrity check is always on and thus the primary keys referenced by the foreign keys in the referencing tables should always exist in the parent table.
 * DB2 Timestamps do not support timezone aware information. Thus a Datetime field including tzinfo(timezone aware info) would fail.

# Feedback/Support

  Your feedback is very much appreciated and expected through project ibm-db:

 * ibm-db issues reports: https://github.com/ibmdb/python-ibmdb/issues

# Contributing to the ibm_db-django python project

  See [CONTRIBUTING](https://github.com/ibmdb/python-ibmdb-django/blob/master/contributing/CONTRIBUTING.md)

  The developer sign-off should include the reference to the DCO in remarks(example below):
  DCO 1.1 Signed-off-by: Random J Developer <random@developer.org>

