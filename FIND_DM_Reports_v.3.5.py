#-------------------------------------------------------------------------------
# Name:        FIND Data Management Report Creator
# Purpose:     Pulls current data from FIND enterprise geodatabase and
#              returns Excel spreadsheet containing a summary of data from the
#              requested report.
# Author:      Molly Moore
# Created:     2016-09-06
# Updated:     2016-09-06 - updated to include options for DM Pending and DM
#              Total reports
#
# To Do List/Future ideas:
# - have script pull from the feature service instead of geodatabase so that
#   everyone can run script
# - include options for other types of reports, such as IDReady, etc.
#
#-------------------------------------------------------------------------------
print "Welcome to FIND Data Mangement Report Creator. You will be prompted " \
"to choose from the following reports:"
print "DM Total - This report will return a summary of all records currently " \
"in the FIND database"
print "DM Ready - This report will return all records in the FIND database " \
"that are ready to be processed by the data management team"
print "DM Pending - This report will return all records that are listed as " \
"DM Pending"
print "DM Biologist - This report will return a separate report for each " \
"biologist that includes the status of all records belonging to them"
print "DM Reviewer - This report will return a separate report for each " \
"ID reviewer that includes information about records ready for them to review"
print "DM All - This report will return all of the aforementioned reports"

# import system modules
import arcpy, os, datetime
from arcpy import env
from arcpy.sa import *

# Set tools to overwrite existing outputs
arcpy.env.overwriteOutput = True

################################################################################
# Define global variables and functions to be used throughout toolbox
################################################################################

# define env.workspace - this space is used for all temporary files
env.workspace = r'in_memory'

# file names of the five element feature classes in the FIND enterprise GDB
input_features = ["FIND3.DBO.el_pt", "FIND3.DBO.el_line", "FIND3.DBO.comm_poly",
"FIND3.DBO.comm_pt", "FIND3.DBO.el_poly", "FIND3.DBO.survey_poly"]

# file names that are used for temporary output element feature classes
elementShapefiles = ["element_point", "element_line", "community_poly",
"community_point", "element_poly", "survey_site"]

# file names that are used for temporary element tables
elementTables = ["element_point1", "element_line1","community_poly1"
"community_point1", "element_poly1", "survey_site1"]

# fields that are kept after joins
keepFields = ["OID", "county", "refcode", "created_by", "created_on", "dm_stat", "dm_stat_comm", "last_up_by", "last_up_on", "element_type", "created_by", "elem_name", "id_prob", "id_prob_comm", "specimen_taken", "specimen_count", "specimen_desc", "curatorial_meth", "specimen_repo", "voucher_photo"]

def countyinfo(elementGDB, counties):
    '''function that assigns county names to elements within county via a
       spatial join'''

    for input, output in zip(input_features, elementShapefiles):
        target_features = os.path.join(elementGDB, input)
        join_features = counties
        element_features = os.path.join(env.workspace, output)

        fieldmappings = arcpy.FieldMappings()
        fieldmappings.addTable(target_features)
        fieldmappings.addTable(counties)

        county = fieldmappings.findFieldMapIndex("COUNTY_NAM")
        fieldmap = fieldmappings.getFieldMap(county)

        field = fieldmap.outputField
        field.name = "county"
        field.aliasName = "County"
        fieldmap.outputField = field

        fieldmap.mergeRule = "first"
        fieldmappings.replaceFieldMap(county, fieldmap)

        for field in fieldmappings.fields:
            if field.name not in keepFields:
                fieldmappings.removeFieldMap(fieldmappings.findFieldMapIndex(field.name))

        # run the spatial join tool
        arcpy.SpatialJoin_analysis(target_features, join_features,
        element_features, "#", "#", fieldmappings)

def elementType():
    for elementShapefile in elementShapefiles:
        input_feature = os.path.join(env.workspace, elementShapefile)

        arcpy.AddField_management(input_feature, "element_type", "TEXT",
        field_length = 15, field_alias = "Element Type")

        arcpy.CalculateField_management(input_feature, "element_type",
        "'" + elementShapefile + "'", "PYTHON_9.3")

def mergetables():
    '''function that converts features classes to tables and merges into one
       table'''

    for input, output in zip(elementShapefiles, elementTables):
        input_features = os.path.join(env.workspace, input)
        arcpy.TableToTable_conversion(input_features, env.workspace, output)

    fieldMappings = arcpy.FieldMappings()
    for table in elementTables:
        fieldMappings.addTable(table)

    for field in fieldMappings.fields:
        if field.name not in keepFields:
            fieldMappings.removeFieldMap(fieldMappings.findFieldMapIndex(field.name))

    merge = os.path.join(env.workspace, "elementRecords")
    arcpy.Merge_management(elementTables, merge, fieldMappings)

def CreatePivotTable(inTable, outTable):
    '''function that creates pivot table'''

    arcpy.Statistics_analysis(inTable, outTable, "refcode COUNT;dm_stat COUNT",
    "refcode;dm_stat")

    arcpy.PivotTable_management(outTable, 'refcode', 'dm_stat', 'FREQUENCY',
    os.path.join(env.workspace, "pivotTable"))

def convertToTable(inFC, surveysite):
    '''converts survey site feature class from FIND GDB into table'''

    # Create FieldMappings object to manage table to table output fields
    fieldMappings = arcpy.FieldMappings()
    fieldMappings.addTable(inFC)

    # Remove all output fields from the field mappings, except fields in query
    survey_keepFields = ["refcode", "dm_stat", "dm_stat_comm"]

    for field in fieldMappings.fields:
        if field.name not in survey_keepFields:
            fieldMappings.removeFieldMap(fieldMappings.findFieldMapIndex(field.name))

    arcpy.TableToTable_conversion(inFC, env.workspace, surveysite,
    "", fieldMappings)

    arcpy.AlterField_management(os.path.join(env.workspace, surveysite), "dm_stat", "survey_site_dmstat", "Survey Site - DM Status")

def manipulateTable(pivotTable):
    with arcpy.da.UpdateCursor(pivotTable, 'refcode') as cursor:
        for row in cursor:
            if row[0] == None:
                cursor.deleteRow()

    # add field
    arcpy.AddField_management(pivotTable, "total_records", "LONG",
    field_length = 3, field_alias = "Total Records")

    # populate total_records field with sum of all records
    expression = "!dmpend! + !dmproc! + !dmready! + !dr! + !idrev!"

    arcpy.CalculateField_management(pivotTable, "total_records",
                                    expression, "PYTHON_9.3")

    join = os.path.join(env.workspace, "surveysite")
    arcpy.JoinField_management(pivotTable, "refcode", join, "refcode",
    ["survey_site_dmstat", "dm_stat_comm"])

    join = os.path.join(env.workspace, "elementRecords")
    arcpy.JoinField_management(pivotTable, "refcode", join, "refcode",
    ["county", "created_by", "created_on", "last_up_by", "last_up_on", "element_type"])

    arcpy.AddField_management(pivotTable, "EastWest", "TEXT", "", "", 1,
    "East West", "", "", "")

    # fill field with E or W depending upon county
    West = ["ERIE", "CRAWFORD", "MERCER", "LAWRENCE", "BEAVER", "WASHINGTON",
    "GREENE", "VENANGO", "BUTLER", "ALLEGHENY", "FAYETTE", "WESTMORELAND",
    "ARMSTORNG", "INDIANA", "CLARION", "JEFFERSON", "FOREST", "WARREN",
    "MCKEAN", "ELK", "CLEARFIELD", "CAMBRIA", "SOMERSET", "BEDFORD", "BLAIR",
    "CENTRE", "CLINTON", "POTTER", "CAMERON", "HUNTINGDON", "FULTON",
    "FRANKLIN"]
    with arcpy.da.UpdateCursor(pivotTable, ["county", "EastWest"]) as cursor:
        for row in cursor:
                if row[0] in West:
                    row[1] = "W"
                    cursor.updateRow(row)
                else:
                    row[1] = "E"
                    cursor.updateRow(row)

    fields = arcpy.ListFields(pivotTable)
    keepFields = ["OID", "county", "refcode", "created_by", "created_on", "dmpend", "dmproc", "dmready", "dr", "idrev", "survey_site_dmstat", "total_records", "dm_stat", "dm_stat_comm", "last_up_by", "last_up_on", "element_type", "created_by"]
    dropFields = [x.name for x in fields if x.name not in keepFields]
    arcpy.DeleteField_management(pivotTable, dropFields)

def dmready(pivotTable):
    arcpy.AddField_management(pivotTable, "ALLREADY", "TEXT", field_length = 1)

    # populate ALLREADY field with Y/N based on whether total records equals DM_Ready
    with arcpy.da.UpdateCursor(pivotTable, ["dmready", "total_records", "survey_site_dmstat",
    "ALLREADY"]) as cursor:
        for row in cursor:
            if row[0] == row[1] and row[2] == "dmready":
                row[3] = "Y"
                cursor.updateRow(row)
            else:
                cursor.deleteRow()

    # delete fields that are not needed - could change these if needed
    deleteFields = ["dr", "idrev", "dmpend", "dmproc", "fc"]
    try:
        deleteFields.append("F")
    except RuntimeError:
        pass
    arcpy.DeleteField_management(pivotTable, deleteFields)

    print "DM Ready Report Created"
    saveExcel(os.path.join(env.workspace, "pivotTable"))

def dmpending(pivotTable):
    with arcpy.da.UpdateCursor(pivotTable, "survey_site_dmstat") as cursor:
        for row in cursor:
            if row[0] == "dmpend":
                pass
            else:
                cursor.deleteRow()

    #deleteFields = ["dr", "idrev", "dmproc", "fc", "dmready"]
    #arcpy.DeleteField_management(pivotTable, deleteFields)
    try:
        arcpy.DeleteField_management(pivotTable, "F")
    except RuntimeError:
        pass

    print "DM Pending Report Created"
    saveExcel(os.path.join(env.workspace, "pivotTable"))

def dmbiologist(pivotTable):
    with arcpy.da.UpdateCursor(pivotTable, ["dmproc", "total_records", "survey_site_dmstat"]) as cursor:
        for row in cursor:
            if row[0] == row[1] and row[2] == "dmproc":
                cursor.deleteRow()
            elif row[0] == row[1] and row[2] is None:
                cursor.deleteRow()
            else:
                pass

    with arcpy.da.UpdateCursor(pivotTable, "created_by") as cursor:
        for row in cursor:
            if row[0] is None:
                pass
            else:
                row[0] = row[0].lower()
                cursor.updateRow(row)

    try:
        arcpy.DeleteField_management(pivotTable, "F")
    except RuntimeError:
        pass

    refname = ["hna", "geo", "lep", "eic", "tra", "dwa", "yea", "zim", "eaz", "alb", "kun", "mcp", "mil", "wis", "gip", "fur", "wal", "wat", "woo", "gle", "gru", "sch", "shc", "dav"]
    createnames = ["ahnatkovich", "bgeorgic", "bleppo", "ceichelberger", "ctracey", "dwatts", "dyeany", "ezimmerman", "ezimmerman", "jalbert", "jkunsman", "jmcpherson", "rmiller", "jwisgo", "kgipe", "mfuredi", "mwalsh", "dwatts", "pwoods", "rgleason", "sgrund", "sschuette", "sschuette", "ezimmerman"]
    for ref, name in zip(refname, createnames):
        with arcpy.da.UpdateCursor(pivotTable, ["refcode", "created_by"]) as cursor:
            for row in cursor:
                if row[0] is None or row[1] is None:
                    pass
                else:
                    if (row[1].lower() == "arcgis" or row[1].lower() == "tjadmin" or row[1].lower() == "administrator" or row[1].lower() == "bgeorgic" or row[1].lower() == "jalbert") and ref in row[0].lower():
                        row[1] = name
                        cursor.updateRow(row)

    outPath = "P:\\Conservation Programs\\Natural Heritage Program\\Data Management" \
    "\\Instructions, procedures and documentation\\FIND\\FIND_2016\\Reports\\Biologist Status Reports"

    with arcpy.da.SearchCursor(pivotTable, "created_by") as cursor:
        biologists = sorted({row[0] for row in cursor})

    for biologist in biologists:
        if biologist is None:
            pass
        else:
            expression = "created_by = '{}'".format(biologist)
            tableTEMP = arcpy.TableToTable_conversion(pivotTable, "in_memory", "tableTEMP", expression)
            filename = biologist + " - " + "FIND Status Report " + time.strftime("%d%b%Y")+".xls"
            outTable = os.path.join(outPath, filename)
            arcpy.TableToExcel_conversion(tableTEMP, outTable)
    print "DM Biologist Report Created!"

def idreview(elementRecords):

    with arcpy.da.UpdateCursor(elementRecords, ["dm_stat"]) as cursor:
        for row in cursor:
            if row[0] != "idrev":
                cursor.deleteRow()
            else:
                pass

    ETtableEXCEL = "P:\\Conservation Programs\\Natural Heritage Program\\Data Management\\Instructions, procedures and documentation\\FIND\\FIND_2016\\DM Documentation\\Admin and Maintenance\\20160209_ET.xlsx\\Final$"
    arcpy.TableToTable_conversion (ETtableEXCEL, "in_memory", "ETtable")
    arcpy.JoinField_management(elementRecords, "elem_name", "in_memory\\ETtable", "ELEMENT_SUBNATIONAL_ID", ["ELEMENT_CODE", "SCIENTIFIC_NAME"])

    arcpy.AddField_management(elementRecords, "EastWest", "TEXT", "", "", 1,
    "East West", "", "", "")

    # fill field with E or W depending upon county
    West = ["ERIE", "CRAWFORD", "MERCER", "LAWRENCE", "BEAVER", "WASHINGTON",
    "GREENE", "VENANGO", "BUTLER", "ALLEGHENY", "FAYETTE", "WESTMORELAND",
    "ARMSTORNG", "INDIANA", "CLARION", "JEFFERSON", "FOREST", "WARREN",
    "MCKEAN", "ELK", "CLEARFIELD", "CAMBRIA", "SOMERSET", "BEDFORD", "BLAIR",
    "CENTRE", "CLINTON", "POTTER", "CAMERON", "HUNTINGDON", "FULTON",
    "FRANKLIN"]
    with arcpy.da.UpdateCursor(elementRecords, ["county", "EastWest"]) as cursor:
        for row in cursor:
                if row[0] in West:
                    row[1] = "W"
                    cursor.updateRow(row)
                else:
                    row[1] = "E"
                    cursor.updateRow(row)

    with arcpy.da.UpdateCursor(elementRecords, "ELEMENT_CODE") as cursor:
        for row in cursor:
            if row[0] is None:
                cursor.deleteRow()
            else:
                pass

    arcpy.AddField_management(elementRecords, "Reviewer", "TEXT", "", "", 35,
    "ID Reviewer", "", "", "")

    with arcpy.da.UpdateCursor(elementRecords, ['ELEMENT_CODE', 'EastWest', 'Reviewer']) as cursor:
        for row in cursor:
            if row[0].startswith('P') and row[1] == "E":
                row[2] = "jkunsman"
            elif row[0].startswith('P') and row[1] == "W":
                row[2] = "sgrund"
            elif row[0].startswith('N'):
                row[2] = "sschuette"
            elif (row[0].startswith('C') or row[0].startswith('H') or row[0].startswith('G')):
                row[2] = "ezimmerman"
            elif row[0].startswith('AB') and row[1] == "E":
                row[2] = "dwatts"
            elif row[0].startswith('AB') and row[1] == "W":
                row[2] = "dyeany"
            elif (row[0].startswith('AM') or (row[0].startswith('AR') or row[0].startswith('AA')) and row[1] == "E"):
                row[2] = "ceichelberger"
            elif ((row[0].startswith('AR') or row[0].startswith('AA')) and row[1] == "W"):
                row[2] = "rmiller"
            elif row[0].startswith('AF') and row[1] == "E":
                row[2] = "Need Reviewer"
            elif row[0].startswith('AF') and row[1] == "W":
                row[2] = "Need Reviewer"
            elif (row[0].startswith('IMBIV') or row[0].startswith('IMGAS')):
                row[2] = "mwalsh"
            elif (row[0].startswith('IILE') or row[0].startswith('IIODO')) and row[1] == "E":
                row[2] = "bleppo"
            elif (row[0].startswith('IILE') or row[0].startswith('IIODO')) and row[1] == "W":
                row[2] = "pwoods"
            elif row[0].startswith('IILAR'):
                row[2] = "cbier"
            elif row[0].startswith('II') and row[1] == "E":
                row[2] = "bleppo"
            elif row[0].startswith('II') and row[1] == "W":
                row[2] = "pwoods"
            elif row[0].startswith('I') and row[1] == "E":
                row[2] = "bleppo"
            elif row[0].startswith('I') and row[1] == "W":
                row[2] = "pwoods"
            else:
                pass
            cursor.updateRow(row)

    fields = arcpy.ListFields(elementRecords)
    keepFields = ["OID", "county", "refcode", "created_by", "created_on", "dm_stat", "Reviewer", "dm_stat_comm", "last_up_by", "last_up_on", "element_type", "id_prob", "id_prob_comm", "specimen_taken", "specimen_count", "specimen_desc", "curatorial_meth", "specimen_repo", "voucher_photo", "SCIENTIFIC_NAME", "ELEMENT_CODE"]
    dropFields = [x.name for x in fields if x.name not in keepFields]
    arcpy.DeleteField_management(elementRecords, dropFields)

    outPath = "P:\\Conservation Programs\\Natural Heritage Program\\Data Management\\Instructions, procedures and documentation\\FIND\\FIND_2016\\Reports\\ID Reviewers Status Reports"

    with arcpy.da.SearchCursor(elementRecords, "Reviewer") as cursor:
        reviewers = sorted({row[0] for row in cursor})

    for reviewer in reviewers:
        if reviewer is None:
            pass
        else:
            expression = "Reviewer = '{}'".format(reviewer)
            tableTEMP = arcpy.TableToTable_conversion(elementRecords, "in_memory", "tableTEMP", expression)
            filename = reviewer + " - " + "ID Reviewers Status Report " + time.strftime("%d%b%Y")+".xls"
            outTable = os.path.join(outPath, filename)
            arcpy.TableToExcel_conversion(tableTEMP, outTable)
    print "ID Reviewers Status Report Created!"

def dmtotal(pivotTable):
    with arcpy.da.UpdateCursor(pivotTable, ["dmproc", "total_records", "survey_site_dmstat"]) as cursor:
        for row in cursor:
            if row[0] == row[1] and row[2] == "dmproc":
                cursor.deleteRow()
            elif row[0] == row[1] and row[2] is None:
                cursor.deleteRow()
            else:
                pass
    saveExcel(pivotTable)

def saveExcel(pivotTable):
    # save output as excel file on the P: drive with date in filename-----------
    outPath = "P:\Conservation Programs\Natural Heritage Program\Data Management" \
    "\Instructions, procedures and documentation\FIND\FIND_2016\Reports"

    if reportType.lower() == "dm pending":
        filename = "DM Pending " + time.strftime("%d%b%Y")+".xls"
    elif reportType.lower() == "dm ready":
        filename = "DM Ready " + time.strftime("%d%b%Y")+".xls"
    elif reportType.lower() == "dm total":
        filename = "DM Total " + time.strftime("%d%b%y")+".xls"

    outTable = os.path.join(outPath, filename)
    arcpy.TableToExcel_conversion(pivotTable, outTable)

################################################################################
# Start Script...
################################################################################

elementGDB = r"Database Connections\\FIND3.Default.pgh-gis.sde"
counties = r"W:\\LYRS\\Boundaries_Political\\County Hollow.lyr"

reportType = raw_input("Which report would you like to run? (please enter 'DM Pending', 'DM Ready', 'DM Biologist', 'DM Reviewer', or 'DM Total')")

print "Getting county info..."
countyinfo(elementGDB, counties)

elementType()

print "Merging tables..."
mergetables()

convertToTable(os.path.join(elementGDB, "FIND3.DBO.survey_poly"), "surveysite")

if reportType.lower() == "dm reviewer":
    print "Creating ID Reviewers Report"
    idreview(os.path.join(env.workspace, "elementRecords"))
else:
    print "Creating pivot table..."
    CreatePivotTable(os.path.join(env.workspace, "elementRecords"), os.path.join(env.workspace, "summaryStats"))

    print "Creating report..."
    manipulateTable(os.path.join(env.workspace, "pivotTable"))


    if reportType.lower() == "dm pending":
        dmpending(os.path.join(env.workspace, "pivotTable"))

    elif reportType.lower() == "dm ready":
        dmready(os.path.join(env.workspace, "pivotTable"))

    elif reportType.lower() == "dm biologist":
        dmbiologist(os.path.join(env.workspace, "pivotTable"))

    elif reportType.lower() == "dm total":
        print "DM Total Report Created"
        dmtotal(os.path.join(env.workspace, "pivotTable"))

