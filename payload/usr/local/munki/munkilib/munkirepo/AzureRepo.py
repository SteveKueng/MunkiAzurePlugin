# encoding: utf-8
"""
AzureRepo.py
"""
from __future__ import absolute_import, print_function

import os
import hashlib
from multiprocessing.pool import ThreadPool
from functools import partial
from xml.parsers.expat import ExpatError

from munkilib.munkirepo import Repo, RepoError
from munkilib.wrappers import get_input
from munkilib.wrappers import get_input, readPlist, readPlistFromString, writePlistToString, PlistReadError

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

__version__ = '1.3'
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

class MakeCatalogsError(Exception):
    '''Error to raise when there is problem making catalogs'''
    pass

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
            # get token from environment
            sas_token = os.getenv('SAS_TOKEN')

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
                 
    def _process_pkgsinfo(self, pkgsinfo_blob, output_fn=None):
        '''Processes pkginfo file and returns a dictionary of catalogs'''
        error = None
        pkginfo_ref = os.path.basename(pkgsinfo_blob.name)
        # Try to read the pkginfo file
        try:
            data = self.container_client.get_blob_client(pkgsinfo_blob).download_blob().readall()
            pkginfo = readPlistFromString(data)
        except BaseException as err:
            error = "Unexpected error for %s: %s" % (pkginfo_ref, err)
            return None, None, error

        if not 'name' in pkginfo:
            error = "WARNING: %s is missing name" % pkginfo_ref

        # don't copy admin notes to catalogs.
        if pkginfo.get('notes'):
            del pkginfo['notes']
        # strip out any keys that start with "_"
        # (example: pkginfo _metadata)
        for key in list(pkginfo.keys()):
            if key.startswith('_'):
                del pkginfo[key]

        return pkginfo_ref, pkginfo, error
    
    def _process_icon_hash(self, icon_blob, output_fn=None):
        '''Processes icon hashes and returns a dictionary'''
        error = None
        icon = None

        name = os.path.basename(icon_blob.name)
        if name == '_icon_hashes.plist':
            return None, icon, error
        if output_fn:
            output_fn("Hashing %s..." % (name))
        try:
            icondata = self.container_client.get_blob_client(icon_blob).download_blob().readall()
        except BaseException as err:
            error = "Unexpected error for %s: %s" % (name, err)
            print(error)
            return name, icon, error
        icon = hashlib.sha256(icondata).hexdigest()
        return name, icon, error
    
    def _verify_pkginfo(self, pkginfo_ref, pkginfo, pkgs_list, errors):
        '''Returns True if referenced installer items are present,
        False otherwise. Adds errors/warnings to the errors list'''
        installer_type = pkginfo.get('installer_type')
        if installer_type in ['nopkg', 'apple_update_metadata']:
            # no associated installer item (pkg) for these types
            return True
        if pkginfo.get('PackageCompleteURL') or pkginfo.get('PackageURL'):
            # installer item may be on a different server
            return True

        if not 'installer_item_location' in pkginfo:
            errors.append("WARNING: %s is missing installer_item_location"
                        % pkginfo_ref)
            return False
        
        # Try to form a path and fail if the
        # installer_item_location is not a valid type
        try:
            installeritempath = os.path.join(pkginfo['installer_item_location'])
        except TypeError:
            errors.append("WARNING: invalid installer_item_location in %s"
                        % pkginfo_ref)
            return False

        # Check if the installer item actually exists
        if not installeritempath in pkgs_list:
            # do a case-insensitive comparison
            found_caseinsensitive_match = False
            for repo_pkg in pkgs_list:
                if installeritempath.lower() == repo_pkg.lower():
                    errors.append(
                        "WARNING: %s refers to installer item: %s. "
                        "The pathname of the item in the repo has "
                        "different case: %s. This may cause issues "
                        "depending on the case-sensitivity of the "
                        "underlying filesystem."
                        % (pkginfo_ref,
                        pkginfo['installer_item_location'], repo_pkg))
                    found_caseinsensitive_match = True
                    break
            if not found_caseinsensitive_match:
                errors.append(
                    "WARNING: %s refers to missing installer item: %s"
                    % (pkginfo_ref, pkginfo['installer_item_location']))
                return False

        #uninstaller sanity checking
        uninstaller_type = pkginfo.get('uninstall_method')
        if uninstaller_type in ['AdobeCCPUninstaller']:
            # uninstaller_item_location is required
            if not 'uninstaller_item_location' in pkginfo:
                errors.append(
                    "WARNING: %s is missing uninstaller_item_location"
                    % pkginfo_ref)
                return False

        # if an uninstaller_item_location is specified, sanity-check it
        if 'uninstaller_item_location' in pkginfo:
            try:
                uninstalleritempath = os.path.join(
                    "pkgs", pkginfo['uninstaller_item_location'])
            except TypeError:
                errors.append("WARNING: invalid uninstaller_item_location "
                            "in %s" % pkginfo_ref)
                return False

            # Check if the uninstaller item actually exists
            if not uninstalleritempath in pkgs_list:
                # do a case-insensitive comparison
                found_caseinsensitive_match = False
                for repo_pkg in pkgs_list:
                    if uninstalleritempath.lower() == repo_pkg.lower():
                        errors.append(
                            "WARNING: %s refers to uninstaller item: %s. "
                            "The pathname of the item in the repo has "
                            "different case: %s. This may cause issues "
                            "depending on the case-sensitivity of the "
                            "underlying filesystem."
                            % (pkginfo_ref,
                            pkginfo['uninstaller_item_location'], repo_pkg))
                        found_caseinsensitive_match = True
                        break
                if not found_caseinsensitive_match:
                    errors.append(
                        "WARNING: %s refers to missing uninstaller item: %s"
                        % (pkginfo_ref, pkginfo['uninstaller_item_location']))
                    return False

        # if we get here we passed all the checks
        return True

    def itemlist(self, kind):
        '''Returns a list of identifiers for each item of kind.
        Kind might be 'catalogs', 'manifests', 'pkgsinfo', 'pkgs', or 'icons'.
        For a file-backed repo this would be a list of pathnames.'''
        itemlist = []
        if kind == 'pkgs':
            kind = 'pkgs/'
        try:
            blob_list = self.container_client.list_blob_names(name_starts_with=kind)
        except ResourceNotFoundError as e:
            raise('Container (%s) not found. Please check the base url.' % kind)
        except AzureError as e:
            raise('Error connecting to Azure: %s' % e)

        for blob in blob_list:
            name = os.path.relpath(blob, kind)
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
        
    def makecatalogs(self, options, output_fn=None):
        '''Calls makecatalogs with options and output_fn.'''
        errors = []
        
        # read all icons
        try:
            blob_list = self.container_client.list_blobs(name_starts_with='icons')
        except ResourceNotFoundError as e:
            raise('Container icons not found. Please check the base url.')
        except AzureError as e:
            raise('Error connecting to Azure: %s' % e)
        
        icons = {}
        tuples = []
        pool = ThreadPool()       
        func = partial(self._process_icon_hash, output_fn=output_fn)
        try:
            tuples = pool.map(func, blob_list)
        except BaseException as err:
            raise MakeCatalogsError('Error processing icon hashes: %s' % err)
        
        for name, icon_hash, error in tuples:
            if error:
                errors.append(error)
            if name:
                icons[name] = icon_hash    
        
        # get a list of pkgsinfo items
        if output_fn:
            output_fn("Getting list of pkgsinfo...")
        try:
            blob_list = self.container_client.list_blobs(name_starts_with='pkgsinfo')
        except RepoError as err:
            raise MakeCatalogsError(
                u"Error getting list of pkgsinfo items: %s" % err)

        # get a list of pkgs items
        if output_fn:
            output_fn("Getting list of pkgs...")
        try:
            pkgs_list = self.itemlist('pkgs')
        except RepoError as err:
            raise MakeCatalogsError(
                u"Error getting list of pkgs items: %s" % err)

        # start with empty catalogs dict
        catalogs = {}
        catalogs['all'] = []
        
        func = partial(self._process_pkgsinfo, output_fn=output_fn)
        try:
            tuples = pool.map(func, blob_list)
        except BaseException as err:
            raise MakeCatalogsError('Error processing pkgsinfo: %s' % err)

        for pkginfo_ref, pkginfo, error in tuples:
            if error:
                errors.append(error)
            if pkginfo:
                # sanity checking
                if not options.skip_payload_check:
                    output_fn("Verifying %s..." % pkginfo_ref)
                    verified = self._verify_pkginfo(pkginfo_ref, pkginfo, pkgs_list, errors)
                    if not verified and not options.force:
                        output_fn("Skipping %s..." % pkginfo_ref)
                        # Skip this pkginfo unless we're running with force flag
                        continue

                output_fn("Adding %s to all..." % pkginfo_ref)
                # append the pkginfo to the relevant catalogs
                catalogs['all'].append(pkginfo)
                for catalogname in pkginfo.get("catalogs", []):
                    if not catalogname:
                        errors.append("WARNING: %s has an empty catalogs array!"
                                    % pkginfo_ref)
                        continue
                    
                    if not catalogname in catalogs:
                        catalogs[catalogname] = []
                    catalogs[catalogname].append(pkginfo)
                    if output_fn:
                        output_fn("Adding %s to %s..." % (pkginfo_ref, catalogname))
    
                # look for catalog names that differ only in case
                duplicate_catalogs = []
                for key in catalogs:
                    if key.lower() in [item.lower() for item in catalogs if item != key]:
                        duplicate_catalogs.append(key)
                if duplicate_catalogs:
                    errors.append("WARNING: There are catalogs with names that differ only "
                                "by case. This may cause issues depending on the case-"
                                "sensitivity of the underlying filesystem: %s"
                                % duplicate_catalogs)

        # clear out old catalogs
        try:
            catalog_list = self.itemlist('catalogs')
        except RepoError:
            catalog_list = []
        for catalog_name in catalog_list:
            if catalog_name not in list(catalogs.keys()):
                catalog_ref = os.path.join('catalogs', catalog_name)
                try:
                    output_fn("Deleting %s..." % catalog_ref)
                    self.delete(catalog_ref)
                except RepoError:
                    errors.append('Could not delete catalog %s' % catalog_name)

        # write the new catalogs
        for key in catalogs:
            catalogpath = os.path.join("catalogs", key)
            if catalogs[key] != "":
                catalog_data = writePlistToString(catalogs[key])
                try:
                    self.put(catalogpath, catalog_data)
                    if output_fn:
                        output_fn("Created %s..." % catalogpath)
                except RepoError as err:
                    errors.append(
                        u'Failed to create catalog %s: %s' % (key, err))
            else:
                errors.append(
                    "WARNING: Did not create catalog %s because it is empty" % key)

        # write icon hashes to the repo                    
        if icons:
            icon_hashes_plist = os.path.join("icons", "_icon_hashes.plist")
            icon_hashes = writePlistToString(icons)
            try:
                self.put(icon_hashes_plist, icon_hashes)
                output_fn("Created %s..." % (icon_hashes_plist))
            except RepoError as err:
                errors.append(
                    u'Failed to create %s: %s' % (icon_hashes_plist, err))
        
        # Return any errors
        return errors
