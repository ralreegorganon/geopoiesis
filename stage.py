import arcpy

arcpy.env.overwriteOutput = True

fgdb = "H:\cddmap\data\catamasswithstructures.gdb\\"
egdb = "C:\Users\jj\AppData\Roaming\ESRI\Desktop10.6\ArcCatalog\cataclysm2@localhost.sde\\"

attributes = [
    ("LANDUSE2005_POLY", "LU05_DESC;LUCODE"),
    ("EOTROADS_ARC", "RDTYPE"),
    ("TOWNSSURVEY_POLYM", "TOWN_ID"),
    ("TRAINS_ARC", "TYPE"),
    ("TRAINS_NODE", "C_RAILSTAT;AMTRAK"),
    ("MBTA_ARC", "GRADE;LINE"),
    ("MBTA_NODE", "TERMINUS;LINE"),
    ("MILITARY_BASES", "COMPONENT;SITE_NAME"),
    ("OCEANMASK_POLY", ""),
    ("TRAILS_ARC", "CLASS"),
]

arcpy.Copy_management(fgdb + "OVERMAP_SUBSET_GRID", egdb + "OVERMAP_SUBSET_GRID")

with arcpy.da.SearchCursor(fgdb + "OVERMAP_SUBSET_GRID", "PageNumber", "PageNumber in (1012, 1013, 1014)") as cursor:
    for row in cursor:
        omid = row[0]

        print("Processing %s" % omid)

        om_fc = "OVERMAP_TERRAIN_GRID_OM%s" % omid
        arcpy.Copy_management(fgdb + om_fc, egdb + om_fc)

        for attribute in attributes:
            tab_fc = "TABULATION_OM%s_%s" % (omid, attribute[0])
            arcpy.Copy_management(fgdb + tab_fc, egdb + tab_fc)