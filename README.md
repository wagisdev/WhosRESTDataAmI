# ArcGIS REST Service Data Source/Metadata Extraction
Simple script that runs through ArcGIS Server based services and lifts the metadata showing which databases they are using, the feature classes in use, etc. It will also send you an email if you are missing a feature class of some variety. It does this by taking the data the services are looking for and comparing it against the database you expect it to be coming from.
