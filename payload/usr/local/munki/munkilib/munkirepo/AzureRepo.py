# encoding: utf-8
"""
AzureRepo.py
"""
from __future__ import absolute_import, print_function

import os
from xml.parsers.expat import ExpatError

from munkilib.munkirepo import Repo
from munkilib.wrappers import get_input
from munkilib.wrappers import get_input, readPlist, PlistReadError


FOUNDATION_SUPPORT = True
try:
    # PyLint cannot properly find names inside Cocoa libraries, so issues bogus
    # No name 'Foo' in module 'Bar' warnings. Disable them.
    # pylint: disable=E0611
    from Foundation import CFPreferencesAppSynchronize, CFPreferencesCopyAppValue, kCFPreferencesCurrentUser, CFPreferencesSetValue, kCFPreferencesAnyHost
    # pylint: enable=E0611
except ImportError:
    # CoreFoundation/Foundation isn't available
    FOUNDATION_SUPPORT = False

try:
    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import AzureError, ResourceNotFoundError, ClientAuthenticationError
except ImportError:
    print('Azure storage library not found. Please install it using "pip install azure-storage-blob"')
    exit(1)

__version__ = '1.0'
BUNDLE_ID = 'com.github.stevekueng.azurerepo'
PREFSNAME = BUNDLE_ID + '.plist'
PREFSPATH = os.path.expanduser(os.path.join('~/Library/Preferences', PREFSNAME))

if FOUNDATION_SUPPORT:
    def get_pref(prefname):
        """Return a preference."""
        return CFPreferencesCopyAppValue(prefname, BUNDLE_ID)
    
    def set_pref(pref_name, pref_value):
        """Sets a preference"""
        try:
            CFPreferencesSetValue(
                pref_name, pref_value, BUNDLE_ID,
                kCFPreferencesCurrentUser, kCFPreferencesAnyHost)
            CFPreferencesAppSynchronize(BUNDLE_ID)
        except BaseException:
            pass

else:
    def get_pref(prefname):
        """Returns a preference for prefname. This is a fallback mechanism if
        CoreFoundation functions are not available -- for example to allow the
        possible use of makecatalogs or manifestutil on Linux"""
        if not hasattr(get_pref, 'cache'):
            get_pref.cache = None
        if not get_pref.cache:
            try:
                get_pref.cache = readPlist(PREFSPATH)
            except (IOError, OSError, ExpatError, PlistReadError):
                get_pref.cache = {}
        if prefname in get_pref.cache:
            return get_pref.cache[prefname]
        # no pref found
        return None

class AzureRepo(Repo):
    '''Class for working with a repo accessible via the MWA2 API'''

    # pylint: disable=super-init-not-called
    def __init__(self, baseurl):
        '''Constructor'''
        self.baseurl = baseurl
        self._connect()
    # pylint: enable=super-init-not-called

    def _connect(self):
        '''connect to the repo. Prompt for credentials if necessary.'''
        sas_token = get_pref('sas_token')
        if not sas_token:
            print('Please provide credentials for %s:' % self.baseurl)
            sas_token = get_input('SAS Token: ')
            choice = get_input('Save credentials? (y/n): ')
            if choice == 'y':
                set_pref('sas_token', sas_token)
            
        # Get the container name from the baseurl and shorten the baseurl
        container_name = os.path.basename(os.path.normpath(self.baseurl))
        self.baseurl = os.path.dirname(self.baseurl)

        try:
            self.blob_client = BlobServiceClient(self.baseurl, credential=sas_token)
            self.container_client = self.blob_client.get_container_client(container=container_name)
            self.container_client.get_account_information()
            print('Connected to Azure Blob Storage')
        except ClientAuthenticationError as e:
            print('Authentication failed. Please check your credentials.')
            set_pref('sas_token', None)
            exit(1)
        except ResourceNotFoundError as e:
            print('Container (%s) not found. Please check the base url.' % container_name)
            exit(1)
        except AzureError as e:
            print('Error connecting to Azure: %s' % e)
            set_pref('sas_token', None)
            exit(1)
                 
    def itemlist(self, kind):
        '''Returns a list of identifiers for each item of kind.
        Kind might be 'catalogs', 'manifests', 'pkgsinfo', 'pkgs', or 'icons'.
        For a file-backed repo this would be a list of pathnames.'''
        itemlist = []
        try:
            blob_list = self.container_client.list_blobs(name_starts_with=kind)
        except ResourceNotFoundError as e:
            raise('Container (%s) not found. Please check the base url.' % kind)
        except AzureError as e:
            raise('Error connecting to Azure: %s' % e)

        for blob in blob_list:
            name = os.path.relpath(blob.name, kind)
            itemlist.append(name)
        return itemlist

    def get(self, resource_identifier):
        '''Returns the content of item with given resource_identifier.
        For a file-backed repo, a resource_identifier of
        'pkgsinfo/apps/Firefox-52.0.plist' would return the contents of
        <repo_root>/pkgsinfo/apps/Firefox-52.0.plist.
        Avoid using this method with the 'pkgs' kind as it might return a
        really large blob of data.'''
        try:
            return self.container_client.get_blob_client(resource_identifier).download_blob().readall()
        except ResourceNotFoundError as e:
            print('Item (%s) not found.' % resource_identifier)
            return None
        except AzureError as e:
            raise('Error connecting to Azure: %s' % e)


    def get_to_local_file(self, resource_identifier, local_file_path):
        '''Gets the contents of item with given resource_identifier and saves
        it to local_file_path.
        For a file-backed repo, a resource_identifier
        of 'pkgsinfo/apps/Firefox-52.0.plist' would copy the contents of
        <repo_root>/pkgsinfo/apps/Firefox-52.0.plist to a local file given by
        local_file_path.'''
        try:
            with open(local_file_path, 'wb') as f:
                self.container_client.get_blob_client(resource_identifier).download_blob().readinto(f)
        except ResourceNotFoundError as e:
            print('Item (%s) not found.' % resource_identifier)
        except AzureError as e:
            raise('Error connecting to Azure: %s' % e)

    def put(self, resource_identifier, content):
        '''Stores content on the repo based on resource_identifier.
        For a file-backed repo, a resource_identifier of
        'pkgsinfo/apps/Firefox-52.0.plist' would result in the content being
        saved to <repo_root>/pkgsinfo/apps/Firefox-52.0.plist.'''
        try: 
            self.container_client.upload_blob(name=resource_identifier, data=content, overwrite=True)
        except AzureError as e:
            raise('Error connecting to Azure: %s' % e)

    def put_from_local_file(self, resource_identifier, local_file_path):
        '''Copies the content of local_file_path to the repo based on
        resource_identifier. For a file-backed repo, a resource_identifier
        of 'pkgsinfo/apps/Firefox-52.0.plist' would result in the content
        being saved to <repo_root>/pkgsinfo/apps/Firefox-52.0.plist.'''
        try:
            with open(local_file_path, 'rb') as f:
                self.container_client.upload_blob(name=resource_identifier, data=f, overwrite=True)
        except AzureError as e:
            raise('Error connecting to Azure: %s' % e)

    def delete(self, resource_identifier):
        '''Deletes a repo object located by resource_identifier.
        For a file-backed repo, a resource_identifier of
        'pkgsinfo/apps/Firefox-52.0.plist' would result in the deletion of
        <repo_root>/pkgsinfo/apps/Firefox-52.0.plist.'''
        try:
            self.container_client.delete_blob(resource_identifier)
        except ResourceNotFoundError as e:
            print('Item (%s) not found.' % resource_identifier)
        except AzureError as e:
            raise('Error connecting to Azure: %s' % e)