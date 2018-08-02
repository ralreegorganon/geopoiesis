using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Dapper;
using Microsoft.Data.Sqlite;

namespace geopoiesis
{
    class Program
    {
        static void Main(string[] args)
        {
            WriteSqlite();
        }

        private static void WriteOvermaps()
        {
            var completeSql = @"
                with
                temp_landuse as 
                (
	                select 
		                pagenumber, LU05_DESC, lucode, percentage, 
		                max(percentage) over (partition by pagenumber) oid_max,
		                max(case when lu05_desc = 'Water' then 1 else 0 end) over (partition by pagenumber) water_precedence 
	                from om{0}_landuse_intersection_tabulation
                ), 
                max_landuse as
                (
	                select * from temp_landuse where (percentage = oid_max and water_precedence = 0) or (water_precedence = 1 and lu05_desc = 'Water')
                ),
                temp_road as 
                (
	                select pagenumber, rdtype, min(rdtype) over (partition by pagenumber) oid_max from om{0}_road_intersection_tabulation
                ), 
                max_road as
                (
	                select * from temp_road where rdtype = oid_max --and rdtype <=4
                ),
                attributed as
                (
	                select 
		                ml.pagenumber, 
		                case when rdtype is null then lucode else 999 end lucode,
		                case when rdtype is null then LU05_DESC else 'Road' end lu05_desc
	                from max_landuse ml left outer join max_road mr on ml.pagenumber = mr.pagenumber
                ),
                quantized as
                (
	                select 
		                otg.pagenumber, lucode, lu05_desc
	                from
		                om{0}_overmap_terrain_grid otg
		                left outer join attributed a
			                on otg.pagenumber = a.pagenumber
                ),
                translated as
                (
                select 
	                pagenumber,
	                lu05_desc,
	                case 
		                when lu05_desc = 'Brushland/Successional' then 'field'		
		                when lu05_desc = 'Cemetery' then 'cemetery_small_north'	
		                when lu05_desc = 'Commercial' then 's_gun'	
		                when lu05_desc = 'Cranberry Bog' then 'pond_swamp'
		                when lu05_desc = 'Cropland' then 'farm_1'	
		                when lu05_desc = 'Forest' then 'forest'	
		                when lu05_desc = 'Forested Wetland' then 'forest_water'	
		                when lu05_desc = 'Golf Course' then 'park'
		                when lu05_desc = 'High Density Residential' then 'house'		
		                when lu05_desc = 'Industrial' then 'small_storage_units'		
		                when lu05_desc = 'Junkyard' then 'toxic_dump'
		                when lu05_desc = 'Low Density Residential' then 'house'	
		                when lu05_desc = 'Marina' then 'fishing_pond_0_0'
		                when lu05_desc = 'Medium Density Residential' then 'house'	
		                when lu05_desc = 'Mining' then 'mine'
		                when lu05_desc = 'Multi-Family Residential' then 'apartments_con_tower_NE'
		                when lu05_desc = 'Non-Forested Wetland' then 'pond_swamp'	
		                when lu05_desc = 'Nursery' then 'orchard_tree_apple'
		                when lu05_desc = 'Open Land' then 'field'	
		                when lu05_desc = 'Orchard' then 'orchard_tree_apple'
		                when lu05_desc = 'Participation Recreation' then 'park'		
		                when lu05_desc = 'Pasture' then 'farm_1'
		                when lu05_desc = 'Powerline/Utility' then 'pwr_sub_s'	
		                when lu05_desc = 'Road' then 'road_ns' 
		                when lu05_desc = 'Saltwater Sandy Beach' then 'dirtlot'
		                when lu05_desc = 'Saltwater Wetland' then 'pond_swamp'
		                when lu05_desc = 'Spectator Recreation' then 'gym'
		                when lu05_desc = 'Transitional' then 'field'	
		                when lu05_desc = 'Transportation' then 'field'
		                when lu05_desc = 'Urban Public/Institutional' then 'police_north'	
		                when lu05_desc = 'Very Low Density Residential' then 'house' 
		                when lu05_desc = 'Waste Disposal' then 'sewage_treatment' 
		                when lu05_desc = 'Water' then 'river'     
		                when lu05_desc = 'Water-Based Recreation' then 'fishing_pond_0_0'		   
		                else 'river'
	                end	omt
	                from quantized
                )
                select omt from translated order by pagenumber
                ";

            using (var connection = new SqlConnection("Server=localhost;Database=Cataclysm;Trusted_connection=true"))
            {
                connection.Open();

                var oms = connection.Query(@"
                    select pagename, pagenumber, pagenumber/69 as y, pagenumber%69 as x from overmap_filtered_grid  
                    order by pagenumber
                    ");
                foreach (var om in oms)
                {
                    string sql = string.Format(completeSql, om.pagenumber);
                    var result = connection.Query(sql);

                    OvermapTerrain current = null;

                    var list = new List<OvermapTerrain>();

                    var derp = 0;
                    foreach (var r in result)
                    {
                        derp++;
                        if (r.omt == current?.Type)
                        {
                            current.Count++;
                        }
                        else
                        {
                            if (current != null)
                            {
                                list.Add(current);
                            }

                            current = new OvermapTerrain
                            {
                                Type = r.omt,
                                Count = 1
                            };
                        }
                    }
                    list.Add(current);

                    var inner = string.Join(",", list.Select(x => $"[\"{x.Type}\",{x.Count}]"));
                    var outer = $",[{inner}]";

                    var template = File.ReadAllLines("omtemplate.txt");
                    template[12] = outer;

                    File.WriteAllLines($@"F:\code\cpp\Cataclysm-DDA\save\Hacks\o.{om.x}.{om.y}", template);
                }
            }
        }

        private static void WriteSqlite()
        {
            var sqlitePath = @"H:\cddmap\cataclysm.sqlite3";

            File.Delete(sqlitePath);

            using (var sourceConnection = new SqlConnection("Server=localhost;Database=Cataclysm;Trusted_connection=true"))
            using (var targetConnection = new SqliteConnection($"Data Source={sqlitePath}"))
            {
                targetConnection.Open();
                targetConnection.Execute("create table omt (om_x int, om_y int, omt_x int, omt_y int, landuse_lucode int, road_rdtype int, subway_grade int, subway_line text, rapidtransit_grade int, rapidtransit_line text, transitstation_terminus text, transitstation_line text, militarybase_component text, militarybase_sitename text, townsurvey_town_id int, trails_class int, trains_type int, trainstation_c_railstat text, trainstation_amtrak text, oceanmask_isocean int)");

                sourceConnection.Open();
                var oms = sourceConnection.Query(@"
                    select pagename, pagenumber, pagenumber/69 as y, pagenumber%69 as x from overmap_subset_grid
                    order by pagenumber
                    ");

                using (var tx = targetConnection.BeginTransaction())
                using(var cmd = targetConnection.CreateCommand())
                {
                    cmd.Transaction = tx;

                    cmd.CommandText = @"
                        insert into omt 
                        (om_x, om_y, omt_x, omt_y, landuse_lucode, road_rdtype, subway_grade, subway_line, rapidtransit_grade, rapidtransit_line, transitstation_terminus, transitstation_line, militarybase_component, militarybase_sitename, townsurvey_town_id, trails_class, trains_type, trainstation_c_railstat, trainstation_amtrak, oceanmask_isocean) 
                        values 
                        ($om_x, $om_y, $omt_x, $omt_y, $landuse_lucode, $road_rdtype, $subway_grade, $subway_line, $rapidtransit_grade, $rapidtransit_line, $transitstation_terminus, $transitstation_line, $militarybase_component, $militarybase_sitename, $townsurvey_town_id, $trails_class, $trains_type, $trainstation_c_railstat, $trainstation_amtrak, $oceanmask_isocean)
                    ";

                    var om_x = cmd.CreateParameter();
                    om_x.ParameterName = "$om_x";
                    var om_y = cmd.CreateParameter();
                    om_y.ParameterName = "$om_y";
                    var omt_x = cmd.CreateParameter();
                    omt_x.ParameterName = "$omt_x";
                    var omt_y = cmd.CreateParameter();
                    omt_y.ParameterName = "$omt_y";
                    var landuse_lucode = cmd.CreateParameter();
                    landuse_lucode.ParameterName = "$landuse_lucode";
                    var road_rdtype = cmd.CreateParameter();
                    road_rdtype.ParameterName = "$road_rdtype";
                    var subway_grade = cmd.CreateParameter();
                    subway_grade.ParameterName = "$subway_grade";
                    var subway_line = cmd.CreateParameter();
                    subway_line.ParameterName = "$subway_line";
                    var rapidtransit_grade = cmd.CreateParameter();
                    rapidtransit_grade.ParameterName = "$rapidtransit_grade";
                    var rapidtransit_line = cmd.CreateParameter();
                    rapidtransit_line.ParameterName = "$rapidtransit_line";
                    var transitstation_terminus = cmd.CreateParameter();
                    transitstation_terminus.ParameterName = "$transitstation_terminus";
                    var transitstation_line = cmd.CreateParameter();
                    transitstation_line.ParameterName = "$transitstation_line";
                    var militarybase_component = cmd.CreateParameter();
                    militarybase_component.ParameterName = "$militarybase_component";
                    var militarybase_sitename = cmd.CreateParameter();
                    militarybase_sitename.ParameterName = "$militarybase_sitename";
                    var townsurvey_town_id = cmd.CreateParameter();
                    townsurvey_town_id.ParameterName = "$townsurvey_town_id";
                    var trails_class = cmd.CreateParameter();
                    trails_class.ParameterName = "$trails_class";
                    var trains_type = cmd.CreateParameter();
                    trains_type.ParameterName = "$trains_type";
                    var trainstation_c_railstat = cmd.CreateParameter();
                    trainstation_c_railstat.ParameterName = "$trainstation_c_railstat";
                    var trainstation_amtrak = cmd.CreateParameter();
                    trainstation_amtrak.ParameterName = "$trainstation_amtrak";
                    var oceanmask_isocean = cmd.CreateParameter();
                    oceanmask_isocean.ParameterName = "$oceanmask_isocean";

                    cmd.Parameters.AddRange(new [] { om_x , om_y, omt_x, omt_y, landuse_lucode, road_rdtype, subway_grade, subway_line, rapidtransit_grade, rapidtransit_line, transitstation_terminus, transitstation_line, militarybase_component, militarybase_sitename, townsurvey_town_id, trails_class, trains_type, trainstation_c_railstat, trainstation_amtrak, oceanmask_isocean });

                    cmd.Prepare();

                    foreach (var om in oms)
                    {
                        var sql = $@"
with
temp_landuse2005_poly as 
(
	select 
		pagenumber, LU05_DESC, lucode, percentage, 
		max(percentage) over (partition by pagenumber) oid_max,
		max(case when lu05_desc = 'Water' then 1 else 0 end) over (partition by pagenumber) water_precedence 
	from tabulation_om{om.pagenumber}_landuse2005_poly
), 
max_landuse2005_poly as
(
	select *, rank() over (partition by pagenumber order by lucode desc) tiebreaker from temp_landuse2005_poly where (percentage = oid_max and water_precedence = 0) or (water_precedence = 1 and lu05_desc = 'Water')
),
tiebreaker_landuse2005_poly as
(
    select * from max_landuse2005_poly where tiebreaker = 1
),
temp_eotroads_arc as 
(
	select pagenumber, rdtype, min(rdtype) over (partition by pagenumber) oid_max from tabulation_om{om.pagenumber}_eotroads_arc
), 
max_eotroads_arc as
(
	select * from temp_eotroads_arc where rdtype = oid_max 
),
temp_mbta_arc_subwayonly as 
(
	select *, max(length) over (partition by pagenumber) oid_max from tabulation_om{om.pagenumber}_mbta_arc where grade = 7
), 
max_mbta_arc_subwayonly as
(
	select * from temp_mbta_arc_subwayonly where length = oid_max 
),
temp_mbta_arc_nosubway as 
(
	select *, max(length) over (partition by pagenumber) oid_max from tabulation_om{om.pagenumber}_mbta_arc where grade <> 7
), 
max_mbta_arc_nosubway as
(
	select * from temp_mbta_arc_nosubway where length = oid_max 
),
max_mbta_node as 
(
	select pagenumber, string_agg(terminus, '/') terminus, string_agg(line, '/') line from tabulation_om{om.pagenumber}_mbta_node group by pagenumber 
), 
temp_military_bases as 
(
	select *, rank() over (partition by pagenumber order by site_name desc) tiebreaker from tabulation_om{om.pagenumber}_military_bases
), 
max_military_bases as 
(
	select * from temp_military_bases where tiebreaker = 1
), 
temp_townssurvey_polym as 
(
	select *, max(percentage) over (partition by pagenumber) oid_max from tabulation_om{om.pagenumber}_townssurvey_polym
), 
max_townssurvey_polym as
(
	select * from temp_townssurvey_polym where percentage = oid_max 
),
temp_trails_arc as 
(
	select *, max(length) over (partition by pagenumber) oid_max from tabulation_om{om.pagenumber}_trails_arc
), 
max_trails_arc as
(
	select * from temp_trails_arc where length = oid_max 
),
temp_trains_arc as 
(
	select *, max(length) over (partition by pagenumber) oid_max from tabulation_om{om.pagenumber}_trains_arc where type <> 9
), 
max_trains_arc as
(
	select *, rank() over (partition by pagenumber order by type desc) tiebreaker from temp_trains_arc where length = oid_max 
),
tiebreaker_trains_arc as
(
	select * from max_trains_arc where tiebreaker = 1
),
max_trains_node as 
(
	select * from tabulation_om{om.pagenumber}_trains_node
), 
max_oceanmask_poly as 
(
	select * from tabulation_om{om.pagenumber}_oceanmask_poly
), 
attributed as
(
	select 
		otg.pagenumber,
		{om.pagenumber}/69 as om_y,
		{om.pagenumber}%69 as om_x,
		(otg.pagenumber - 1)/180 as omt_y,
		(otg.pagenumber - 1)%180 as omt_x,
		ml.lucode as landuse_lucode,
		mr.rdtype as road_rdtype,
		mmbtas.grade as subway_grade,
		mmbtas.line as subway_line,
		mmbtans.grade as rapidtransit_grade,
		mmbtans.line as rapidtransit_line,
		mmbtan.terminus as transitstation_terminus,
		mmbtan.line as transitstation_line,
		mmb.component as militarybase_component,
		mmb.site_name as militarybase_sitename,
		mts.town_id as townsurvey_town_id,
		mta.class as trails_class,
		mtrainza.type as trains_type,
		mtrainzn.c_railstat as trainstation_c_railstat,
		mtrainzn.amtrak as trainstation_amtrak,
		case when ml.lucode is null and mop.pagenumber is not null then 1 else 0 end as oceanmask_isocean
	from 
		overmap_terrain_grid_om{om.pagenumber} otg
		left outer join tiebreaker_landuse2005_poly ml 
			on otg.pagenumber = ml.pagenumber
		left outer join max_eotroads_arc mr 
			on otg.pagenumber = mr.pagenumber
		left outer join max_mbta_arc_subwayonly mmbtas
			on otg.pagenumber = mmbtas.pagenumber
		left outer join max_mbta_arc_nosubway mmbtans
			on otg.pagenumber = mmbtans.pagenumber
		left outer join max_mbta_node mmbtan
			on otg.pagenumber = mmbtan.pagenumber
		left outer join max_military_bases mmb
			on otg.pagenumber = mmb.pagenumber
		left outer join max_townssurvey_polym mts
			on otg.pagenumber = mts.pagenumber
		left outer join max_trails_arc mta
			on otg.pagenumber = mta.pagenumber
		left outer join tiebreaker_trains_arc mtrainza
			on otg.pagenumber = mtrainza.pagenumber
		left outer join max_trains_node mtrainzn
			on otg.pagenumber = mtrainzn.pagenumber
		left outer join max_oceanmask_poly mop
			on otg.pagenumber = mop.pagenumber
)
select * from attributed 
order by omt_y, omt_x
                        
                        ";
                        var sources = sourceConnection.Query(sql);

                        foreach (var source in sources)
                        {
                            om_x.Value = source.om_x;
                            om_y.Value = source.om_y;
                            omt_x.Value = source.omt_x;
                            omt_y.Value = source.omt_y;
                            landuse_lucode.Value = source.landuse_lucode ?? DBNull.Value;
                            road_rdtype.Value = source.road_rdtype ?? DBNull.Value;
                            subway_grade.Value = source.subway_grade ?? DBNull.Value;
                            subway_line.Value = source.subway_line ?? DBNull.Value;
                            rapidtransit_grade.Value = source.rapidtransit_grade ?? DBNull.Value;
                            rapidtransit_line.Value = source.rapidtransit_line ?? DBNull.Value;
                            transitstation_terminus.Value = source.transitstation_terminus ?? DBNull.Value;
                            transitstation_line.Value = source.transitstation_line ?? DBNull.Value;
                            militarybase_component.Value = source.militarybase_component ?? DBNull.Value;
                            militarybase_sitename.Value = source.militarybase_sitename ?? DBNull.Value;
                            townsurvey_town_id.Value = source.townsurvey_town_id ?? DBNull.Value;
                            trails_class.Value = source.trails_class ?? DBNull.Value;
                            trains_type.Value = source.trains_type ?? DBNull.Value;
                            trainstation_c_railstat.Value = source.trainstation_c_railstat ?? DBNull.Value;
                            trainstation_amtrak.Value = source.trainstation_amtrak ?? DBNull.Value;
                            oceanmask_isocean.Value = source.oceanmask_isocean ?? DBNull.Value;

                            cmd.ExecuteNonQuery();
                        }
                    }

                    tx.Commit();
                }
            }
        }
    }

    public class OvermapTerrain
    {
        public string Type { get; set; }
        public int Count { get; set; }
    }
}
