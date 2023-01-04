#-------------------------------------------------------------------------------
# Name:  ArcGIS REST Service Metadata Extraction
# Purpose:  Capture critical data about your REST services to include data
#           sources, publishing docs, and more.
#
# Author:      John Spence
#
# Created:  December 9, 2022
# Modified:
# Modification Purpose:
#
#
#
#-------------------------------------------------------------------------------


# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Set the data store location for all your MAPXs below.
# ShareLevel options include PUBLIC or PRIVATE.
# ShareOrg options include SHARE_ORGANIZATION or NO_SHARE_ORGANIZATION
# ShareGroups options are to place the name of a group in there.
#
# ------------------------------- Dependencies ---------------------------------
#
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Script Type
scriptType = 'Missing REST Service Asset(s)'

# Signin Config
serverURL = r'https://yourinternalfacingurl.com/arcgis'  #Internal facing URL that can reach the admin side of things.
serverPubURLSub = r'https://yourexternalfacingurl/arcgis'
serverTokenURL = r'https://yourinternalfacingurl.com/arcgis/tokens/' #URL for getting an auth token.
serverUSR = r'youradminusername'
serverPAS = r'youradminpassword'
serverTokenExpire = r'90'
serverTokenClient = r'requestip'
serverTokenClient = r''

# Where the Output will reside from the search
inCsv = r'\\whereyoustoreyourfiles\GISPublished\Production'

# File Name Pre-Fix for the Output
fileprefix = 'PROD_'

# Expected GDB Connection File
dbConnection = [('YourEnterpriseDB', r'C:\Users\yourusername\AppData\Roaming\Esri\YourDBConnectionFile.sde')]

# Find Services To Change
affectedFeatureClassName = r'' # Format Database.Owner.FeatureClassName

# Send confirmation of rebuild to
adminNotify = 'gisdba@bellevuewa.gov'
deptAdminNotify = 'gisinfo@bellevuewa.gov'

# Configure the e-mail server and other info here.
mail_server = 'smtp-relay.google.com'
mail_from = 'GIS REST Services <noreply@yourdomain.com>'
mail_subject = '{} Notification: '.format(scriptType)

# Test User Override
testUser = ''

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python Libraries
import arcpy
import os
import csv
import sys
import datetime
import time
import requests
import string
import re
import base64
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import base64
import concurrent.futures
from tqdm import tqdm
from xml.etree import ElementTree

#-------------------------------------------------------------------------------
#
#
#                                 Function
#
#
#-------------------------------------------------------------------------------

def main(inCsv, dbConnection):
#-------------------------------------------------------------------------------
# Name:        Function - main
# Purpose:  Starts the whole thing.
#-------------------------------------------------------------------------------

    starttime = datetime.datetime.now()
    print ('88888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888')
    print ('\nService Configuration Data Capture Started: {}'.format(starttime))
    print ('    * Getting Security Token...')
    securityToken = getToken(serverUSR, serverPAS, serverTokenURL, serverTokenExpire, serverTokenClient, serverTokenClient)
    if securityToken != '':
        print ('    - Security Token Received.')
    else:
        print ('    ! No security token received. Stopping process.')
        sys.exit()
    print ('\n    * Capturing Service Data...')
    adminURL = serverURL + '/admin'
    publicURL = serverURL + '/rest/services'

    inCsv = inCsv + '\\' + fileprefix + 'SVCSources_' + str(datetime.datetime.now().date()) + '.csv'
    dbAssets = findServicesData(adminURL, publicURL, securityToken, affectedFeatureClassName, inCsv, fileprefix)
    print ('\n    * Reviewing Data Sources...')
    print ('        -- Found {} items to check.'.format(len(dbAssets)))
    missingData = checkIfMissingAssets(dbAssets)
    if len(missingData) > 0:
        print ('\n    * Sending Missing Data Source Notice...')
        sendMissingNotice(missingData)
        print ('\n      Missing data. Looks like you have work to do.')
    else:
        print ('\n    Huzzah! Not missing data sources. My work is complete!')
    print ('88888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888')

    return ()


def getToken(userName, password, tokenURL, tokenExpiry, tokenClient, tokenReferer):
#-------------------------------------------------------------------------------
# Name:        Function - getToken
# Purpose:  Captures the security token needed to access secure data/config
#-------------------------------------------------------------------------------

    tokenParams = {'username': userName, 'password': password, 'expiration': tokenExpiry, 'client': tokenClient, 'referer': tokenReferer, 'f': 'json'}
    r = requests.post(tokenURL, data=tokenParams)
    if r.status_code == requests.codes.ok:
        result = r.json()
        return result['token']
    else:
        return ''


def listFolders(adminURL, securityToken):
#-------------------------------------------------------------------------------
# Name:        Function - listFolders
# Purpose:  Get your folders and ignore a few specific ones.
#-------------------------------------------------------------------------------

    servicesURL = adminURL + '/services'
    servicesParams = {'detail': 'false', 'f': 'json'}
    servicesHeader = {'Content-Type': 'application/json', 'X-Esri-Authorization': 'Bearer {}'.format(securityToken)}

    servicesResponse = requests.get(servicesURL, params=servicesParams, headers=servicesHeader)
    if servicesResponse.status_code == requests.codes.ok:
        servicesJson = servicesResponse.json()
        gisFolders = servicesJson['folders']
        gisFolders.remove('System')
        gisFolders.remove('Utilities')
        gisFolders.append('')
        gisFolders.sort(key=str.lower)

        return gisFolders

    else:

        return ()

def listServices(adminURL, folder, securityToken):
#-------------------------------------------------------------------------------
# Name:        Function - listServices
# Purpose:  Get your list of services to capture data from.
#-------------------------------------------------------------------------------

    servicesURL = adminURL + '/services'
    servicesParams = {'detail': 'false', 'f': 'json'}
    servicesHeader = {'Content-Type': 'application/json', 'X-Esri-Authorization': 'Bearer {}'.format(securityToken)}

    if folder:
        servicesListURL = "{}/{}".format(servicesURL, folder)
    else:
        servicesListURL = servicesURL

    servicesResponse = requests.get(servicesListURL, params=servicesParams, headers=servicesHeader)

    if servicesResponse.status_code == requests.codes.ok:
        servicesJson = servicesResponse.json()
        return servicesJson['services']
    else:
        return ()

def findServicesData(adminURL, publicURL, securityToken, affectedFeatureClassName, inCsv, fileprefix):
#-------------------------------------------------------------------------------
# Name:        Function - findServiceData
# Purpose:  Get data behind your services and write it to CSV.
#-------------------------------------------------------------------------------
    
    servicesHeader = {'Content-Type': 'application/json', 'X-Esri-Authorization': 'Bearer {}'.format(securityToken)}
    manifestSuffix = 'iteminfo/manifest/manifest.xml'
    dbAssetPayload = []
   
    servicesFolders = listFolders(adminURL, securityToken)
    for folder in servicesFolders:
        gisServices = listServices(adminURL, folder, securityToken)

        for gisService in gisServices:
            if gisService['type'] in ['MapServer', 'ImageServer', 'FeatureServer']:

                if folder:
                    thisServiceURL = '{}/{}/{}'.format(publicURL, folder, gisService['serviceName'])
                    manifestURL = '{}/services/{}/{}.{}/{}'.format(adminURL, folder, gisService['serviceName'], gisService['type'], manifestSuffix)
                    updatedInfoURL = '{}/services/{}/{}.{}/lifecycleinfos?f=pjson'.format(adminURL, folder, gisService['serviceName'], gisService['type'])
                else:
                    thisServiceURL = '{}/{}'.format(publicURL, gisService['serviceName'])
                    manifestURL = '{}/services/{}.{}/{}'.format(adminURL, gisService['serviceName'], gisService['type'], manifestSuffix)
                    updatedInfoURL = '{}/services/{}.{}/lifecycleinfos?f=pjson'.format(adminURL, gisService['serviceName'], gisService['type'])

                updateResponse = requests.get (updatedInfoURL, headers=servicesHeader)
                updatePayload = updateResponse.json()

                if updateResponse.status_code == requests.codes.ok:
                    lastUpdatedDTRAW = updatePayload['lastmodified']
                    lastUpdatedDTSTG = datetime.datetime.fromtimestamp(lastUpdatedDTRAW/1000)
                    lastUpdatedDT = lastUpdatedDTSTG.strftime('%m/%d/%Y %H:%M:%S')

                #if gisService['serviceName'] == 'FEMAFloodplainComparison':
                manifestResponse = requests.get(manifestURL, headers=servicesHeader)

                if manifestResponse.status_code == requests.codes.ok:
                    payload = ElementTree.fromstring(manifestResponse.content)

                    for data in payload.iter('SVCResource'):
                        sourceDocRAW = data[3].text
                        if '.aprx' in sourceDocRAW or '.mxd' in sourceDocRAW:
                            sourceDoc = sourceDocRAW.rsplit('\\', 1)[1]
                            sourceDocLoc = sourceDocRAW.rsplit('\\', 1)[:1][0]
                        else:
                            sourceDoc = sourceDocRAW
                            sourceDocLoc = ''

                    for item in payload.iter('OnPremiseConnectionString'):
                        dbData = item.text.split(';')
                        if len(dbData) == 1 and 'DATABASE' in dbData[0] and '.gdb' in dbData[0]:
                            dbInstance = '**File Geodatabase**'
                            dbName = dbData[0].replace('DATABASE=', '')
                            for item in payload.iter('SVCDataset'):
                                if '.gdb\\' in item[2].text:
                                    layerID = item[0].text
                                    layerName = item[1].text
                                    layerPathRAW = item[2].text
                                    layerPath = layerPathRAW[layerPathRAW.find('.gdb\\'):]
                                    layerPath = layerPath.replace('.gdb\\','')
                                    layerSVRPath = item[3].text
                                    layerPCKGPath = item[4].text
                                    layerSVRName = item[5].text
                                    layerDataType = item[6].text
                                    if '(query layer)' in layerName:
                                        layerQLPositive = 'Yes'
                                    else:
                                        layerQLPositive = 'No'
                                        
                                    layerServiceURL = thisServiceURL.replace(serverURL, serverPubURLSub)

                                    print ('        Service: {} | {}'.format(folder, gisService['serviceName']))
                                    print ('        -- Last Updated: {}'.format(lastUpdatedDT))
                                    print ('        -- Service Layer Name: {}'.format(layerSVRName))
                                    print ('        -- DB Instance: {}'.format(dbInstance))
                                    print ('        -- DB Name: {}'.format(dbName))
                                    print ('        -- Feature Class: {}'.format(layerPath))
                                    print ('        -- Service URL: {}\n'.format(layerServiceURL))
                        
                                    if not os.path.isfile(inCsv):
                                        csvFile = open(inCsv, 'w', newline='')
                                        try:
                                            writer = csv.writer(csvFile)
                                            writer.writerow(('Folder', 'Service Name', 'Last Updated', 'Layer Name', 'Query Layer', 'Layer Source', 'DB Instance', 'DB Name', 'Source Doc', 'Source Location', 'Service URL'))
                                            writer.writerow((folder, gisService['serviceName'], lastUpdatedDT, layerSVRName,  layerQLPositive, 
                                                                layerPath, dbInstance, dbName, sourceDoc, sourceDocLoc, layerServiceURL))
                                        except:
                                            print ('error writing first row of csv')
                                    else:
                                        csvFile = open(inCsv, 'a', newline='')
                                        try:
                                            writer = csv.writer(csvFile)
                                            writer.writerow((folder, gisService['serviceName'], lastUpdatedDT, layerSVRName,  layerQLPositive, 
                                                                layerPath, dbInstance, dbName, sourceDoc, sourceDocLoc, layerServiceURL))
                                        except Exception as e:
                                            print ('error writing to csv')
                                            print (e)
                        else:
                            if 'DB_CONNECTION_PROPERTIES' in dbData[3] and 'DATABASE' in  dbData[4]:
                                dbInstance = dbData[3].replace('DB_CONNECTION_PROPERTIES=', '')
                                dbName = dbData[4].replace('DATABASE=', '')
                                for item in payload.iter('SVCDataset'):
                                    if '.sde\\' in item[2].text:
                                        layerID = item[0].text
                                        layerName = item[1].text
                                        layerPathRAW = item[2].text
                                        layerPath = layerPathRAW[layerPathRAW.find('.sde\\'):]
                                        layerPath = layerPath.replace('.sde\\','')
                                        layerSVRPath = item[3].text
                                        layerPCKGPath = item[4].text
                                        layerSVRName = item[5].text
                                        layerDataType = item[6].text
                                        if '(query layer)' in layerName:
                                            layerQLPositive = 'Yes'
                                        else:
                                            layerQLPositive = 'No'

                                        layerServiceURL = thisServiceURL.replace(serverURL, serverPubURLSub)
                        
                                        print ('        Service: {} | {}'.format(folder, gisService['serviceName']))
                                        print ('        -- Last Updated: {}'.format(lastUpdatedDT))
                                        print ('        -- Service Layer Name: {}'.format(layerSVRName))
                                        print ('        -- DB Instance: {}'.format(dbInstance))
                                        print ('        -- DB Name: {}'.format(dbName))
                                        print ('        -- Feature Class: {}'.format(layerPath))
                                        print ('        -- Service URL: {}\n'.format(layerServiceURL))
                                        if layerQLPositive != 'Yes':
                                            dbAssetPayload.append(layerPath)                                        
                        
                                        if not os.path.isfile(inCsv):
                                            csvFile = open(inCsv, 'w', newline='')
                                            try:
                                                writer = csv.writer(csvFile)
                                                writer.writerow(('Folder', 'Service Name', 'Last Updated', 'Layer Name', 'Query Layer', 'Layer Source', 'DB Instance', 'DB Name', 'Source Doc', 'Source Location', 'Service URL'))
                                                writer.writerow((folder, gisService['serviceName'], lastUpdatedDT, layerSVRName,  layerQLPositive, 
                                                                    layerPath, dbInstance, dbName, sourceDoc, sourceDocLoc, layerServiceURL))
                                            except:
                                                print ('error writing first row of csv')
                                        else:
                                            csvFile = open(inCsv, 'a', newline='')
                                            try:
                                                writer = csv.writer(csvFile)
                                                writer.writerow((folder, gisService['serviceName'], lastUpdatedDT, layerSVRName,  layerQLPositive, 
                                                                    layerPath, dbInstance, dbName, sourceDoc, sourceDocLoc, layerServiceURL))
                                            except Exception as e:
                                                print ('error writing to csv')
                                                print (e)


    dbAssets = [*set(dbAssetPayload)]

    return (dbAssets)

def getDBFeatureClasses(dbConfig):
#-------------------------------------------------------------------------------
# Name:        Function - getDBFeatureClasses
# Purpose:  Poorly named, but it gets all your SDE database content.
#-------------------------------------------------------------------------------

    arcpy.env.workspace = dbConfig
    dbFeatureClasses = arcpy.ListFeatureClasses()
    dbTables = arcpy.ListTables()
    dbDataSets = arcpy.ListDatasets()
    dbContentPayload = []
    for dbFC in dbFeatureClasses:
        dbContentPayload.append(dbFC.upper())
    for dbTBL in dbTables:
        dbContentPayload.append(dbTBL.upper())
    for dataset in dbDataSets:
        arcpy.env.workspace = os.path.join(dbConfig, dataset)
        dbFeatureClasses = arcpy.ListFeatureClasses()
        dbTables = arcpy.ListTables()
        for dbFC in dbFeatureClasses:
            dbContentPayload.append(dbFC.upper())
        for dbTBL in dbTables:
            dbContentPayload.append(dbTBL.upper())

    return (dbContentPayload)

def checkIfMissingAssets(dbAssets):
#-------------------------------------------------------------------------------
# Name:        Function - checkIfMissingAssets
# Purpose:  Checks what you have in REST services against what is in the DB.
#-------------------------------------------------------------------------------
    missingData = []
    for dbConf in dbConnection:
        dbConfig = dbConf[1]
        db = dbConf[0]
        dbResults = getDBFeatureClasses(dbConfig)
        for dbData in dbAssets:
            if dbData.upper() not in dbResults:
                if '\\' in dbData and '"' not in dbData:
                    dbData = dbData.rsplit('\\', 1)[1]
                    if dbData.upper() not in dbResults:
                        print (dbData)
                        pass
                    else:
                        continue
                print ('    -- Item Missing {}'.format(dbData))
                missingData.append(dbData)
    return (missingData)

def sendMissingNotice(missingData):
#-------------------------------------------------------------------------------
# Name:        Function - sendMissingNotice
# Purpose:  Sends a naughty gram telling you what is missing from the DB.
#-------------------------------------------------------------------------------

    rowOutput = ''
    if len(missingData) != 0:
        for item in missingData:
            featureClass = item
            rowLine = '''
                <tr>
                    <td>{}</td>
                </tr>
            '''.format(featureClass)
            rowOutput = rowOutput + rowLine
        notification = 1
    else:
        print ('    !! No missing item notification requried !!')
        return()

    payLoadHTMLPreStart = '''
    <div>
        <h3 style="font-family:verdana;">Used in REST Service, but missing from database</h3>
    <table>
        <tr>
            <th>Missing Asset</th>
        </tr>
    '''
    payLoadHTMLData = '''
    {}
        </table>
    </div>
    <br>
    '''.format(rowOutput)

    payLoadHTML = payLoadHTMLPreStart + payLoadHTMLData

    print ('\n      * Preparing notification...')

    payLoadHTMLStart = '''
    <html>
    <head>
    <style>
    table {
        font-family: arial, sans-serif;
        border-collapse: collapse;
        width: 100%;
        }
        
    td, th {
        border: 1px solid #dddddd;
        text-align: left;
        padding: 8px;
        }

    tr:nth-child(even) {
        background-color: #dddddd;
        }
        
    </style>
    </head>
    
    <body>
    <!--<h2 style="font-family:verdana;"><b></b></h2>-->
    
    '''

    payLoadHTMLEnd = '''
    <br>
    <div>
    <!--<bold>*Seasonal worker accounts will auto enable when AD user account is enabled.</bold>-->
    </div>
    <div>
    [This is an automated system message. Please contact gisdba@bellevuewa.gov for all questions.]
    </div>
    </body>
    </html>

    '''

    payLoadHTML = payLoadHTMLStart + payLoadHTML + payLoadHTMLEnd


    payLoadTXT = 'HTML Message -- Use HTML Compliant Email'

    partTXT = MIMEText(payLoadTXT, 'plain')
    partHTML = MIMEText(payLoadHTML, 'html')
    msg = MIMEMultipart('alternative')
    msg['Subject'] = mail_subject
    msg['From'] = mail_from
    msg['X-Priority'] = '1' # 1 high, 3 normal, 5 low

    if testUser != '':

        emailContact = testUser

        print ('      Sending data to {}'.format(emailContact))
            
        msg['To'] = emailContact
        msg.attach(partTXT)
        msg.attach(partHTML)

        server = smtplib.SMTP(mail_server)

        server.sendmail(mail_from, [emailContact], msg.as_string())
        server.quit()

    else:

        emailContact = deptAdminNotify

        print ('      Sending data to {}'.format(emailContact))
            
        #msg['To'] = emailContact
        #msg['Cc'] = adminNotify
        msg['To'] = adminNotify
        msg.attach(partTXT)
        msg.attach(partHTML)

        server = smtplib.SMTP(mail_server)

        #server.sendmail(mail_from, [emailContact, adminNotify], msg.as_string())
        server.sendmail(mail_from, [adminNotify], msg.as_string())
        server.quit()

    return()


#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

if __name__ == "__main__":
    main(inCsv, dbConnection)
