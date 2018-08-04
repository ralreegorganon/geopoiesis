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

        const string viewSql = @"
create or alter view attributes_om{0} as
with
temp_landuse2005_poly as 
(
	select 
		pagenumber, LU05_DESC, lucode, percentage, 
		max(percentage) over (partition by pagenumber) oid_max,
		max(case when lu05_desc = 'Water' then 1 else 0 end) over (partition by pagenumber) water_precedence 
	from tabulation_om{0}_landuse2005_poly
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
	select pagenumber, rdtype, min(rdtype) over (partition by pagenumber) oid_max from tabulation_om{0}_eotroads_arc
), 
max_eotroads_arc as
(
	select * from temp_eotroads_arc where rdtype = oid_max 
),
temp_mbta_arc_subwayonly as 
(
	select *, max(length) over (partition by pagenumber) oid_max from tabulation_om{0}_mbta_arc where grade = 7
), 
max_mbta_arc_subwayonly as
(
	select * from temp_mbta_arc_subwayonly where length = oid_max 
),
temp_mbta_arc_nosubway as 
(
	select *, max(length) over (partition by pagenumber) oid_max from tabulation_om{0}_mbta_arc where grade <> 7
), 
max_mbta_arc_nosubway as
(
	select * from temp_mbta_arc_nosubway where length = oid_max 
),
max_mbta_node as 
(
	select pagenumber, string_agg(terminus, '/') terminus, string_agg(line, '/') line from tabulation_om{0}_mbta_node group by pagenumber 
), 
temp_military_bases as 
(
	select *, rank() over (partition by pagenumber order by site_name desc) tiebreaker from tabulation_om{0}_military_bases
), 
max_military_bases as 
(
	select * from temp_military_bases where tiebreaker = 1
), 
temp_townssurvey_polym as 
(
	select *, max(percentage) over (partition by pagenumber) oid_max from tabulation_om{0}_townssurvey_polym
), 
max_townssurvey_polym as
(
	select *, rank() over (partition by pagenumber order by town_id desc) tiebreaker from temp_townssurvey_polym where percentage = oid_max 
),
tiebreaker_townsurvey_polym as
(
    select * from max_townssurvey_polym where tiebreaker = 1
),
temp_trails_arc as 
(
	select *, max(length) over (partition by pagenumber) oid_max from tabulation_om{0}_trails_arc
), 
max_trails_arc as
(
	select * from temp_trails_arc where length = oid_max 
),
temp_trains_arc as 
(
	select *, max(length) over (partition by pagenumber) oid_max from tabulation_om{0}_trains_arc where type <> 9
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
	select pagenumber, max(case when c_railstat = 'Y' then 1 else null end) c_railstat, max(case when amtrak = 'Y' then 1 else null end) amtrak  from tabulation_om{0}_trains_node group by pagenumber
), 
max_oceanmask_poly as 
(
	select * from tabulation_om{0}_oceanmask_poly
), 
attributed as
(
	select 
		otg.pagenumber,
		{0}/69 as om_y,
		{0}%69 as om_x,
		otg.pagenumber/180 as omt_y,
		otg.pagenumber%180 as omt_x,
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
		overmap_terrain_grid_om{0} otg
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
		left outer join tiebreaker_townsurvey_polym mts
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
";

        static void Main(string[] args)
        {
            CreateOrReplaceViews();
            WriteSqlite();
            //WriteOvermaps();
        }

        private static void WriteOvermaps()
        {
            var subwaySql = @"
                select 
                    om_x,
                    om_y,
                    omt_x,
                    omt_y,
                    case 
    	                when transitstation_line is not null and subway_grade is not null then 'sub_station'
    	                when subway_grade is not null then 'subway_east'
                        else 'empty_rock'
                    end as omt 
                from omt  
                where om_x >= 40 and om_x < 45	and om_y >= 10 and om_y < 15
                order by om_y, om_x, omt_y, omt_x

            ";

            var aboveSql = @"
                with
                translated as
                (
                select 
                    *,
                    case 
                        when landuse_lucode = 40 then 'field'		
                        when landuse_lucode = 34 then 'cemetery_small_north'	
                        when landuse_lucode = 15 then 's_gun'	
                        when landuse_lucode = 23 then 'pond_swamp'
                        when landuse_lucode = 1 then 'farm_1'	
                        when landuse_lucode = 3 then 'forest'	
                        when landuse_lucode = 37 then 'forest_water'	
                        when landuse_lucode = 26 then 'park'
                        when landuse_lucode = 11 then 'house'		
                        when landuse_lucode = 16 then 'small_storage_units'		
                        when landuse_lucode = 39 then 'toxic_dump'
                        when landuse_lucode = 13 then 'house'	
                        when landuse_lucode = 29 then 'fishing_pond_0_0'
                        when landuse_lucode = 12 then 'house'	
                        when landuse_lucode = 5 then 'mine'
                        when landuse_lucode = 10 then 'apartments_con_tower_NE'
                        when landuse_lucode = 4 then 'pond_swamp'	
                        when landuse_lucode = 36 then 'orchard_tree_apple'
                        when landuse_lucode = 6 then 'field'	
                        when landuse_lucode = 35 then 'orchard_tree_apple'
                        when landuse_lucode = 7 then 'park'		
                        when landuse_lucode = 2 then 'farm_1'
                        when landuse_lucode = 24 then 'pwr_sub_s'	
                        when landuse_lucode = 25 then 'dirtlot'
                        when landuse_lucode = 14 then 'pond_swamp'
                        when landuse_lucode = 8 then 'gym'
                        when landuse_lucode = 17 then 'field'	
                        when landuse_lucode = 18 then 'field'
                        when landuse_lucode = 31 then 'police_north'	
                        when landuse_lucode = 38 then 'house' 
                        when landuse_lucode = 19 then 'sewage_treatment' 
                        when landuse_lucode = 20 then 'river'     
                        when landuse_lucode = 9 then 'fishing_pond_0_0'		   
                    end	omt,
                    case
                        when road_rdtype is not null then 'road_ns'
                    end road
                    from omt
                )
                select 
                    om_x,
                    om_y,
                    omt_x,
                    omt_y,
                    case 
    	                when trails_class is not null then 'NatureTrail_1a'
                        when transitstation_line is not null and subway_grade is not null then 'sub_station'
    	                when transitstation_line is not null then 'tower_lab_finale'
    	                when trains_type in (1, 2) then 'bank'
                        when trains_type is not null then 'field'
    	                when trainstation_c_railstat = 1 or trainstation_amtrak = 1 then 'lab_train_depot'
    	                when road is not null then road 
    	                when militarybase_sitename is not null then 'bunker'
    	                when omt is not null then omt
    	                when oceanmask_isocean = 1 then 'river'
    	                else 'forest'
	                end as omt 
                from translated  
                order by om_y, om_x, omt_y, omt_x
                ";

            var sqlitePath = @"H:\cddmap\cataclysm2.sqlite3";

            using (var c = new SqliteConnection($"Data Source={sqlitePath}"))
            {
                var result = c.Query<OvermapTerrain>(aboveSql);

                var overmaps = result.GroupBy(x => (x.om_x, x.om_y));

                foreach (var overmap in overmaps)
                {
                    OvermapTerrainGroup current = null;

                    var list = new List<OvermapTerrainGroup>();

                    foreach (var r in overmap.OrderBy(x => x.omt_y).ThenBy(x => x.omt_x))
                    {
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

                                current = new OvermapTerrainGroup
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

                    File.WriteAllLines($@"F:\code\cpp\Cataclysm-DDA\save\Hacks\o.{overmap.Key.Item1}.{overmap.Key.Item2}", template);
                }
            }
        }

        private static void CreateOrReplaceViews()
        {
            using (var c = new SqlConnection("Server=localhost;Database=Cataclysm2;Trusted_connection=true"))
            {
                c.Open();
                var oms = c.Query(@"
                    select pagename, pagenumber, pagenumber/69 as y, pagenumber%69 as x from overmap_subset_grid
                    where pagenumber in (1012, 1013, 1014)
                    order by pagenumber
                    ");

                foreach (var om in oms)
                {
                    var sql = string.Format(viewSql, (int) om.pagenumber);
                    c.Execute(sql);
                }
            }
        }

        private static void WriteSqlite()
        {
            var sqlitePath = @"H:\cddmap\cataclysm2.sqlite3";

            File.Delete(sqlitePath);

            using (var sourceConnection = new SqlConnection("Server=localhost;Database=Cataclysm2;Trusted_connection=true"))
            using (var targetConnection = new SqliteConnection($"Data Source={sqlitePath}"))
            {
                targetConnection.Open();
                targetConnection.Execute("create table omt (om_x int, om_y int, omt_x int, omt_y int, landuse_lucode int, road_rdtype int, subway_grade int, subway_line text, rapidtransit_grade int, rapidtransit_line text, transitstation_terminus text, transitstation_line text, militarybase_component text, militarybase_sitename text, townsurvey_town_id int, trails_class int, trains_type int, trainstation_c_railstat int, trainstation_amtrak int, oceanmask_isocean int)");

                sourceConnection.Open();
                var oms = sourceConnection.Query(@"
                    select pagename, pagenumber, pagenumber/69 as y, pagenumber%69 as x from overmap_subset_grid
                    where pagenumber in (1012, 1013, 1014)
                    order by pagenumber
                    ");

                using(var cmd = targetConnection.CreateCommand())
                {
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
                        using (var tx = targetConnection.BeginTransaction())
                        {
                            cmd.Transaction = tx;
                            var sources = sourceConnection.Query($"select * from attributes_om{om.pagenumber} order by om_x, om_y");

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
                            tx.Commit();
                        }
                    }
                }
            }
        }
    }

    public class OvermapTerrainGroup
    {
        public string Type { get; set; }
        public int Count { get; set; }
    }

    public class OvermapTerrain
    {
        public int om_x { get; set; }
        public int om_y { get; set; }
        public int omt_x { get; set; }
        public int omt_y { get; set; }
        public string omt { get; set; }
    }
}
