# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2018.                                 |
# +--------------------------------------------------------------------------+
# | This module complies with Django 1.0 and is                              |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi              |
# +--------------------------------------------------------------------------+

"""
This module implements command line interface for DB2 through Django.
"""

try:
    from django.db.backends import BaseDatabaseClient
except ImportError:
    from django.db.backends.base.client import BaseDatabaseClient

import types

import os

class DatabaseClient( BaseDatabaseClient ):
    
    #Over-riding base method to provide shell support for DB2 through Django.
    def runshell( self ):
        settings_dict = self.connection.settings_dict
        database_name = settings_dict['NAME']
        database_user = settings_dict['USER']
        database_password = settings_dict['PASSWORD']
            
        cmdArgs = ["db2"]
        
        if ( os.name == 'nt' ):
            cmdArgs += ["db2 connect to %s" % database_name]
        else:
            cmdArgs += ["connect to %s" % database_name]
        if sys.version_info.major >= 3:
            basestring = str
        else:
            basestring = basestring

        if ( isinstance( database_user, basestring ) and 
            ( database_user != '' ) ):
            cmdArgs += ["user %s" % database_user]
            
            if ( isinstance( database_password, basestring ) and 
                ( database_password != '' ) ):
                cmdArgs += ["using %s" % database_password]
                
        # db2cmd is the shell which is required to run db2 commands on windows.
        if ( os.name == 'nt' ):
            os.execvp( 'db2cmd', cmdArgs )
        else:
            os.execvp( 'db2', cmdArgs )
