import arcpy

arcpy.env.workspace = "H:\cddmap\data\catamasswithstructures.gdb"

omg_fc = "in_memory\OVERMAP_GRID"
subset_grid_fc = "OVERMAP_SUBSET_GRID"
single_overmap_fc = "in_memory\SINGLE_OVERMAP"

grid_source_fc = "in_memory\LANDUSE2005_POLY"
subset_source_fc = "in_memory\TOWNSSURVEY_POLYM"

# arcpy.CopyFeatures_management("LANDUSE2005_POLY", grid_source_fc)
# arcpy.CopyFeatures_management("TOWNSSURVEY_POLYM", subset_source_fc)

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
    ("STRUCTURE_POLY", "STRUCT_ID;AREA_SQ_FT"),
]

for source in attributes:
    arcpy.CopyFeatures_management(source[0], "in_memory\\"+source[0])

recreate_if_exists = True

def nuke(fc):
    if arcpy.Exists(fc):
        arcpy.Delete_management(fc)

def clip(omid, source_fc):
    clip_fc = "in_memory\CLIP_OM%s" % omid
    arcpy.Clip_analysis(source_fc, single_overmap_fc, clip_fc)
    return clip_fc

def tabulate(omid, source_fc, clip_fc, fields):
    tab_fc = "TABULATION_OM%s_%s" % (omid, source_fc.replace("in_memory\\",""))
    exists = arcpy.Exists(tab_fc)

    if exists and recreate_if_exists:
        arcpy.Delete_management(tab_fc)
        arcpy.TabulateIntersection_analysis(om_fc_m, "PageNumber", clip_fc, tab_fc, fields)
    elif not exists:
        arcpy.TabulateIntersection_analysis(om_fc_m, "PageNumber", clip_fc, tab_fc, fields)

    arcpy.Delete_management(clip_fc)

def clip_and_tabulate(omid, source_fc, fields):
    clip_fc = clip(omid, source_fc)
    tabulate(omid, source_fc, clip_fc, fields)

print("Creating %s" % omg_fc)
arcpy.GridIndexFeatures_cartography(omg_fc, grid_source_fc, "NO_INTERSECTFEATURE", "", "", "4320 meters", "4320 meters", "", "", "", "0")
print("Done creating %s" % omg_fc)

print("Creating %s" % subset_grid_fc)
nuke(subset_grid_fc)
arcpy.MakeFeatureLayer_management(omg_fc, "BLARG")
arcpy.SelectLayerByLocation_management("BLARG", "INTERSECT", subset_source_fc)
arcpy.CopyFeatures_management("BLARG", subset_grid_fc)
print("Done creating %s" % subset_grid_fc)

arcpy.Delete_management(omg_fc)
arcpy.Delete_management("BLARG")

with arcpy.da.SearchCursor(subset_grid_fc, "PageNumber", "PageNumber in (1012, 1013, 1014)") as cursor:
# with arcpy.da.SearchCursor(subset_grid_fc, "PageNumber") as cursor:
    for row in cursor:
        omid = row[0]

        print("Processing %s" % omid)

        nuke(single_overmap_fc)
        arcpy.MakeFeatureLayer_management(subset_grid_fc, single_overmap_fc, "PageNumber = %s" % omid)

        om_fc = "OVERMAP_TERRAIN_GRID_OM%s" % omid
        om_fc_m = "in_memory\\" + om_fc

        arcpy.GridIndexFeatures_cartography(om_fc_m, single_overmap_fc, "NO_INTERSECTFEATURE", "", "", "24 meters", "24 meters", "", "180", "180", "0")

        for attribute in attributes:
            clip_and_tabulate(omid, "in_memory\\"+attribute[0], attribute[1])

        om_fc_exists = arcpy.Exists(om_fc)
        if om_fc_exists and recreate_if_exists:
            arcpy.Delete_management(om_fc)
            arcpy.CopyFeatures_management(om_fc_m, om_fc)
        elif not om_fc_exists:
            arcpy.CopyFeatures_management(om_fc_m, om_fc)

        nuke(om_fc_m)