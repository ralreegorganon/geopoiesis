import arcpy

arcpy.env.workspace = "H:\cddmap\data\catamass.gdb"

omg_fc = "OVERMAP_GRID"
subset_grid_fc = "OVERMAP_SUBSET_GRID"
single_overmap_fc = "SINGLE_OVERMAP"

grid_source_fc = "LANDUSE2005_POLY"
subset_source_fc = "TOWNSSURVEY_POLYM"

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

recreate_if_exists = False

def nuke(fc):
    if arcpy.Exists(fc):
        arcpy.Delete_management(fc)

def clip(omid, source_fc):
    clip_fc = "CLIP_OM%s_%s" % (omid, source_fc)
    exists = arcpy.Exists(clip_fc)

    if exists and recreate_if_exists:
        arcpy.Delete_management(clip_fc)
        arcpy.Clip_analysis(source_fc, single_overmap_fc, clip_fc)
    elif not exists:
        arcpy.Clip_analysis(source_fc, single_overmap_fc, clip_fc)
    
    return clip_fc

def tabulate(omid, source_fc, clip_fc, fields):
    tab_fc = "TABULATION_OM%s_%s" % (omid, source_fc)
    exists = arcpy.Exists(tab_fc)

    if exists and recreate_if_exists:
        arcpy.Delete_management(tab_fc)
        arcpy.TabulateIntersection_analysis(om_fc, "PageNumber", clip_fc, tab_fc, fields)
    elif not exists:
        arcpy.TabulateIntersection_analysis(om_fc, "PageNumber", clip_fc, tab_fc, fields)

def clip_and_tabulate(omid, source_fc, fields):
    clip_fc = clip(omid, source_fc)
    tabulate(omid, source_fc, clip_fc, fields)

print("Creating %s" % omg_fc)
nuke(omg_fc)
arcpy.GridIndexFeatures_cartography(omg_fc, grid_source_fc, "NO_INTERSECTFEATURE", "", "", "4320 meters", "4320 meters")
print("Done creating %s" % omg_fc)

print("Creating %s" % subset_grid_fc)
nuke(subset_grid_fc)
arcpy.MakeFeatureLayer_management(omg_fc, "BLARG")
arcpy.SelectLayerByLocation_management("BLARG", "INTERSECT", subset_source_fc)
arcpy.CopyFeatures_management("BLARG", subset_grid_fc)
print("Done creating %s" % subset_grid_fc)

# with arcpy.da.SearchCursor(subset_grid_fc, "PageNumber", "PageNumber = 1013") as cursor:
with arcpy.da.SearchCursor(subset_grid_fc, "PageNumber") as cursor:
    for row in cursor:
        omid = row[0]

        print("Processing %s" % omid)

        nuke(single_overmap_fc)
        arcpy.MakeFeatureLayer_management(subset_grid_fc, single_overmap_fc, "PageNumber = %s" % omid)

        om_fc = "OVERMAP_TERRAIN_GRID_OM%s" % omid
        om_fc_exists = arcpy.Exists(om_fc)

        if om_fc_exists and recreate_if_exists:
            arcpy.Delete_management(om_fc)
            arcpy.GridIndexFeatures_cartography(om_fc, single_overmap_fc, "NO_INTERSECTFEATURE", "", "", "24 meters", "24 meters", "", "180", "180")
        elif not om_fc_exists:
            arcpy.GridIndexFeatures_cartography(om_fc, single_overmap_fc, "NO_INTERSECTFEATURE", "", "", "24 meters", "24 meters", "", "180", "180")

        for attribute in attributes:
            clip_and_tabulate(omid, attribute[0], attribute[1])