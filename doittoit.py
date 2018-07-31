import arcpy

arcpy.env.workspace = "C:\users\jj\desktop\custom.gdb"
#arcpy.env.workspace = "C:\Users\jj\AppData\Roaming\ESRI\Desktop10.6\ArcCatalog\cataclysm@localhost.sde"

omg_fc = "OVERMAP_GRID"
landuse_fc = "LANDUSE2005_POLY"
road_fc = "EOTROADS_ARC"
towns_fc = "TOWNSSURVEY_POLYM"
filtered_grid_fc = "OVERMAP_FILTERED_GRID"
single_overmap_fc = 'SINGLE_OVERMAP'

print("Creating %s" % omg_fc)
if arcpy.Exists(omg_fc):
    arcpy.Delete_management(omg_fc)
arcpy.GridIndexFeatures_cartography(omg_fc, landuse_fc, "NO_INTERSECTFEATURE", "", "", "4320 meters", "4320 meters")
print("Done creating %s" % omg_fc)

print("Creating %s" % filtered_grid_fc)
if arcpy.Exists(filtered_grid_fc):
    arcpy.Delete_management(filtered_grid_fc)
arcpy.MakeFeatureLayer_management(omg_fc, "BLARG")
arcpy.SelectLayerByLocation_management("BLARG", "INTERSECT", towns_fc)
arcpy.CopyFeatures_management("BLARG", filtered_grid_fc)
print("Done creating %s" % filtered_grid_fc)

with arcpy.da.SearchCursor(filtered_grid_fc, "PageNumber", "PageNumber = 1013") as cursor:
# with arcpy.da.SearchCursor(filtered_grid_fc, "PageNumber") as cursor:
    for row in cursor:
        omid = row[0]
        if arcpy.Exists(single_overmap_fc):
            arcpy.Delete_management(single_overmap_fc)
        arcpy.MakeFeatureLayer_management(filtered_grid_fc, single_overmap_fc, "PageNumber = %s" % omid)
        
        om_fc = "OM%s_OVERMAP_TERRAIN_GRID" % omid
        print("Creating %s" % om_fc)
        if arcpy.Exists(om_fc):
            arcpy.Delete_management(om_fc)
        arcpy.GridIndexFeatures_cartography(om_fc, single_overmap_fc, "NO_INTERSECTFEATURE", "", "", "24 meters", "24 meters", "", "180", "180")
        print("Done creating %s" % om_fc)

        luc_fc = "OM%s_LANDUSE_CLIP" % omid
        print("Creating %s" % luc_fc)
        if arcpy.Exists(luc_fc):
            arcpy.Delete_management(luc_fc)
        arcpy.Clip_analysis(landuse_fc, single_overmap_fc, luc_fc)
        print("Done creating %s" % luc_fc)

        luit_fc = "OM%s_LANDUSE_INTERSECTION_TABULATION" % omid
        print("Creating %s" % luit_fc)
        if arcpy.Exists(luit_fc):
            arcpy.Delete_management(luit_fc)
        arcpy.TabulateIntersection_analysis(om_fc, "PageNumber", luc_fc, luit_fc, "LU05_DESC;LUCODE")
        print("Done creating %s" % luit_fc)

        rc_fc = "OM%s_ROAD_CLIP" % omid
        print("Creating %s" % rc_fc)
        if arcpy.Exists(rc_fc):
            arcpy.Delete_management(rc_fc)
        arcpy.Clip_analysis(road_fc, single_overmap_fc, rc_fc)
        print("Done creating %s" % rc_fc)

        rit_fc = "OM%s_ROAD_INTERSECTION_TABULATION" % omid
        print("Creating %s" % rit_fc)
        if arcpy.Exists(rit_fc):
            arcpy.Delete_management(rit_fc)
        arcpy.TabulateIntersection_analysis(om_fc, "PageNumber", rc_fc, rit_fc, "RDTYPE")
        print("Done creating %s" % rit_fc)
